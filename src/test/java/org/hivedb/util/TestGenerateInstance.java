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
	@Test
	public void testGenerateAndCopyProperties() {
		Boo boo = (Boo)new GenerateInstance<Boo>(Boo.class).generate();
		Boo boohoo = new GenerateInstance<Boo>(Boo.class).generateAndCopyProperties(boo);
		Assert.assertEquals(boo, boohoo);
		Assert.assertNotSame(boo.getString(), boohoo.getString());
		Assert.assertNotSame(boo.getCoos(), boohoo.getCoos());
		Assert.assertEquals(new HashSet(boo.getCoos()), new HashSet(boohoo.getCoos()));
		Assert.assertNotSame(Atom.getFirstOrThrow(boo.getCoos()), Atom.getFirstOrThrow(boohoo.getCoos()));
		Assert.assertEquals(Atom.getFirstOrThrow(boo.getCoos()), Atom.getFirstOrThrow(boohoo.getCoos()));
	}
}
