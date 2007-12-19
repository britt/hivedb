package org.hivedb.util;

import java.util.Collection;

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
}
