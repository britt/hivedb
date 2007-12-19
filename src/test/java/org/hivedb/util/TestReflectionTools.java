package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.hivedb.util.ReflectionTools;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestReflectionTools  {
	
	
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
