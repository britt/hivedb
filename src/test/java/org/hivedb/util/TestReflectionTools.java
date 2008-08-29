package org.hivedb.util;

import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.classgen.ReflectionTools.SetterWrapper;
import org.junit.Assert;
import org.junit.Test;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class TestReflectionTools  {
	
	@Test
	public void testGetPropertyNameOfAccessor() {
		Assert.assertEquals(
				ReflectionTools.getPropertyNameOfAccessor(
						ReflectionTools.getGetterOfProperty(Foo.class, "stroo")),
				"stroo");
				
	}
	@Test
	public void testIsGetter() {
		Assert.assertTrue(
				ReflectionTools.isGetter(
					ReflectionTools.getGetterOfProperty(Foo.class, "stroo").getName()));
		try {
			Assert.assertFalse(
					ReflectionTools.isGetter(
							Foo.class.getMethod("obtainCircumlocution", new Class[] {String.class})));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Assert.assertFalse(
				ReflectionTools.isGetter(
					ReflectionTools.getSetterWrapperOfProperty(Foo.class, "orama").getRealSetter().getName()));
	}
	@Test
	public void testIsSetter() {
		Assert.assertFalse(
				ReflectionTools.isSetter(
						ReflectionTools.getGetterOfProperty(Foo.class, "stroo").getName()));
		try {
			Assert.assertFalse(
					ReflectionTools.isSetter(
							Foo.class.getMethod("obtainCircumlocution", new Class[] {String.class})));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Assert.assertTrue(
				ReflectionTools.isSetter(
						ReflectionTools.getSetterWrapperOfProperty(Foo.class, "orama").getRealSetter().getName()));
	}
	@Test
	public void testDoesSetterExist() {
		Assert.assertTrue(
						ReflectionTools.doesRealSetterExist(
								ReflectionTools.getGetterOfProperty(Foo.class, "orama")));
		Assert.assertFalse(
				ReflectionTools.doesRealSetterExist(
						ReflectionTools.getGetterOfProperty(Foo.class, "stroo")));
	}
	@Test
	public void testGetCorrespondingSetterWrapper() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		SetterWrapper setterWrapper = ReflectionTools.getCorrespondingSetterWrapper(foo, "getStroo", String.class);
		setterWrapper.invoke(foo, "x");
		Assert.assertEquals(foo.getStroo(), "x");
		
		setterWrapper = ReflectionTools.getCorrespondingSetterWrapper(ReflectionTools.getGetterOfProperty(Foo.class, "stroo"));
		setterWrapper.invoke(foo, "y");
		Assert.assertEquals(foo.getStroo(), "y");
	}
	@Test
	public void testGetCorrespondingGetter() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		Method getter = ReflectionTools.getCorrespondingGetter(foo, "setStroo");
		try {
			Assert.assertEquals(getter.invoke(foo, new Object[] {}), foo.getStroo());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void testGetGetterOfProperty() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		Method getter = ReflectionTools.getGetterOfProperty(Foo.class, "stroo");
		try {
			Assert.assertEquals(getter.invoke(foo, new Object[] {}), foo.getStroo());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void testGetSetterWrapperOfProperty() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		SetterWrapper setterWrapper = ReflectionTools.getSetterWrapperOfProperty(Foo.class, "stroo");
		setterWrapper.invoke(foo, "x");
		Assert.assertEquals(foo.getStroo(), "x");
	}
	
	@Test
	public void testGetSetterOfProperty() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		Method setter = ReflectionTools.getSetterOfProperty(Foo.class, "orama");
		try {
			setter.invoke(foo, "x");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Assert.assertEquals(foo.getOrama(), "x");
	}
	
	@Test
	public void testGetDeclaredPublicMethods() {
		try {
			Assert.assertEquals(
					new HashSet(ReflectionTools.getDeclaredPublicMethods(Foo.class)).hashCode(),
					new HashSet(Arrays.asList(new Method[] {
							Foo.class.getMethod("getStroo", new Class[] {}),
							Foo.class.getMethod("getIoo", new Class[] {}),
							Foo.class.getMethod("getCoos", new Class[] {}),
							Foo.class.getMethod("getShoos", new Class[] {}),
							Foo.class.getMethod("obtainCircumlocution", new Class[] {String.class}),
							Foo.class.getMethod("setOrama", new Class[] {String.class}),
							Foo.class.getMethod("getOrama", new Class[] {}),	
					})).hashCode());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	@Test
	public void testDoesImplementOrExtend() {
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(
				GeneratedClassFactory.getGeneratedClass(Foo.class),
				Foo.class));
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(
				Fooer.class,
				Foo.class));
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(
				Fooimp.class,
				Foo.class));
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(
				new Class[] {Fooimp.class, Boo.class},
				Foo.class));
		Assert.assertFalse(ReflectionTools.doesImplementOrExtend(
				new Class[] {Boo.class, Coo.class},
				Foo.class));
	}
	@Test
	public void testWhichIsImplemented() {
		Assert.assertEquals(
			ReflectionTools.whichIsImplemented(
				Fooimp.class,
				Arrays.asList(new Class[] {Foo.class, Boo.class, Coo.class})),
			Foo.class);
	}
	
	@Test
	public void testGetNullFields() {	
		try {
			Assert.assertEquals(
					new HashSet(
							ReflectionTools.getNullFields( GeneratedClassFactory.newInstance(Foo.class), Foo.class )).hashCode(),
					new HashSet(Arrays.asList(new Method[] {
							Foo.class.getMethod("getStroo", new Class[] {}),
							Foo.class.getMethod("getIoo", new Class[] {}),
							Foo.class.getMethod("getCoos", new Class[] {}),
							Foo.class.getMethod("getShoos", new Class[] {}),
							Foo.class.getMethod("getOrama", new Class[] {}),	
					})).hashCode());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void testInvokeGetters() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		Collection<Method> getters = ReflectionTools.getGetters(Foo.class);
		Collection objects = ReflectionTools.invokeGetters(foo, ReflectionTools.getGetters(Foo.class));
		Iterator<Object> iterator = objects.iterator();
		for (Method getter : getters) {
			Object obj = iterator.next();
			try {
				Assert.assertEquals(obj, getter.invoke(foo, new Object[] {}));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	@Test
	public void testInvokeGetter() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		
		Object obj = ReflectionTools.invokeGetter(foo, "stroo");
		Assert.assertEquals(obj, foo.getStroo());
	}
	@Test
	public void testInvokeSetter() {
		Foo foo = new GenerateInstance<Foo>(Foo.class).generate();
		ReflectionTools.invokeSetter(foo, "stroo", "x");
		Assert.assertEquals("x", foo.getStroo());
	}
	
	@Test
	public void testGetCollectionType() {
		Assert.assertEquals(ReflectionTools.getCollectionItemType(Foo.class, "coos"), Coo.class);
		// Assert that extended interfaces can get the generic type
		Assert.assertEquals(ReflectionTools.getCollectionItemType(Fooer.class, "coos"), Coo.class);
		// Assert that implementations can get the generic type
		Assert.assertEquals(ReflectionTools.getCollectionItemType(Fooimp.class, "coos"), Coo.class);
		// Assert that subclasses can get the generic type
		Assert.assertEquals(ReflectionTools.getCollectionItemType(Fooimpimp.class, "coos"), Coo.class);
	}
	
	@Test
	public void testGetUniqueComplexPropertyTypes() {
//		Collection<Class<?>> classes = new HashSet<Class<?>>(ReflectionTools.getUniqueComplexPropertyTypes(Arrays.asList(new Class[] {Foo.class})));
//		Assert.assertEquals(classes, new HashSet<Class<?>>((Collection<? extends Class<?>>) Arrays.asList(new Class[] {Foo.class,Coo.class,Boo.class})));
	}
	private interface Foo {
		String getStroo();
		Integer getIoo();
		Collection<Coo> getCoos();
		Collection<Short> getShoos();
		String obtainCircumlocution(String puppy);
		void setOrama(String rama);
		String getOrama();
	}
	private interface Coo {
		Float getFloo();
		Collection<Boo> getBoos();
	}
	private interface Boo {
		Double getDoo();
	}
	private interface Fooer extends Foo {
		String getWhatever();
	}
	private class Fooimp implements Foo {

		public Fooimp() {
			
		}
		public Collection<Coo> getCoos() {
			return null;
		}

		public Integer getIoo() {
			return null;
		}

		public Collection<Short> getShoos() {
			return null;
		}

		public String getStroo() {
			return null;
		}

		public String getOrama() {
			return null;
		}

		public String obtainCircumlocution(String puppy) {
			return null;
		}

		public void setOrama(String rama) {
		}

	}
	private class Fooimpimp extends Fooimp {

		public Integer getIoo() {
			return null;
		}

		public Collection<Short> getShoos() {
			return null;
		}

		public String getStroo() {
			return null;
		}}
}
