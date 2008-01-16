package org.hivedb.util;

import java.util.Collection;
import java.util.HashSet;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.Ignore;
import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGenerateInstance {
	@GeneratedClass("FooImpl")
	public interface Foo {
		@Ignore
		String getString();
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
		Baa boohaa = new GenerateInstance<Baa>(Baa.class).generateAndCopyProperties(baa);
		Assert.assertEquals(baa, boohaa);
		Assert.assertSame(baa.getString(), boohaa.getString()); // "primitives" are not cloned
		Assert.assertNotSame(baa.getCaas(), boohaa.getCaas()); // collections are
		Assert.assertEquals(new HashSet(baa.getCaas()), new HashSet(boohaa.getCaas())); // should still equal
		Assert.assertNotSame(Atom.getFirstOrThrow(baa.getCaas()), Atom.getFirstOrThrow(boohaa.getCaas())); // complex items clone
		Assert.assertEquals(Atom.getFirstOrThrow(baa.getCaas()), Atom.getFirstOrThrow(boohaa.getCaas())); // should still equal
	}
}
