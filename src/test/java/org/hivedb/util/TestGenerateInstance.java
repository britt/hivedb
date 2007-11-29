package org.hivedb.util;

import java.util.Collection;

import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGenerateInstance {
	interface Foo {
		int getInt();
		Collection<Boo> getBoos();
	}
	interface Boo {
		String getString();
		Coo getCoo();
	}
	interface Coo {
		long getLong();
	}
	
	@Test
	public void testGenerateInstance() {
		Foo foo = (Foo)new GenerateInstance<Foo>(Foo.class).generate();
		Assert.assertTrue(foo.getInt() != 0);
		Assert.assertTrue(foo.getBoos().size() > 0);
		Boo boo = Atom.getFirstOrThrow(foo.getBoos());
		Assert.assertTrue(boo.getString() != "" && boo.getString() != null);
		Assert.assertTrue(boo.getCoo() != null);
		Assert.assertTrue(boo.getCoo().getLong() != 0l);
	}
}
