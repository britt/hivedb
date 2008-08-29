package org.hivedb.util;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.GeneratorIgnore;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.functional.Atom;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

public class TestGenerateInstance {
	@GeneratedClass("FooImpl")
	public interface Foo {
		@GeneratorIgnore
		String getString();
    void setString(String s);
    int getInt();
	}
	@GeneratedClass("BooImpl")
	public interface Boo extends Foo {
		Collection<Coo> getCoos();
	}
	@GeneratedClass("CooImpl")
	public interface Coo {
		long getLong();
	}
	
	@Test
	public void testGenerateInstance() {
		Boo boo = (Boo)new GenerateInstance<Boo>(Boo.class).generate();
		Assert.assertTrue(boo.getInt() != 0);
		Assert.assertTrue(boo.getCoos().size() > 0);
		Assert.assertTrue(boo.getString() == null);
	}

  @Test
  public void shouldAddSetters() throws Exception {
    Foo generated = GeneratedClassFactory.newInstance(Foo.class, new GeneratedInstanceInterceptor(Foo.class));
    String speech = "I have a dream!";
    generated.setString(speech);
    Assert.assertEquals(generated.getString(), speech);
  }

  @GeneratedClass("FaaImpl")
	public interface Faa {
		String getString();
		int getInt();
	}
	@GeneratedClass("BaaImpl")
	public interface Baa extends Faa {
		Collection<Caa> getCaas();
	}
	@GeneratedClass("CaaImpl")
	public interface Caa {
		long getLong();
	}
	@Test
	public void testGenerateAndCopyProperties() {
		Baa baa = (Baa)new GenerateInstance<Baa>(Baa.class).generate();
		Object x = baa;
		// Test getter via method invocation
		try {
			final int int1 = baa.getInt();
			Assert.assertEquals(x.getClass().getMethod("getInt", new Class[] {}).invoke(baa, new Object[] {}), int1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Baa boohaa = new GenerateInstance<Baa>(Baa.class).generateAndCopyProperties(baa);
		Assert.assertEquals(baa, boohaa);
		Assert.assertSame(baa.getString(), boohaa.getString()); // "primitives" are not cloned
		Assert.assertNotSame(baa.getCaas(), boohaa.getCaas()); // collections are
		Assert.assertEquals(new HashSet(baa.getCaas()), new HashSet(boohaa.getCaas())); // should still equal
		Assert.assertNotSame(Atom.getFirstOrThrow(baa.getCaas()), Atom.getFirstOrThrow(boohaa.getCaas())); // complex items clone
		Assert.assertEquals(Atom.getFirstOrThrow(baa.getCaas()), Atom.getFirstOrThrow(boohaa.getCaas())); // should still equal
	}
}
