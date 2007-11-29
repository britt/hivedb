package org.hivedb.util;

import java.util.Collection;

import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBeanGenerator {

	public static class Foo {
		
		private int foo;
		private Collection<Boo> boos;
		public int getInt() {return foo;}
		public Collection<Boo> getBoos(){ return boos;}

		public void setInt(int foo) {
			this.foo = foo;
		}
		public void setBoos(Collection<Boo> boos) {
			this.boos = boos;
		}
	}
	
	public static class Boo {
		private String string;
		private Coo coo;
		public String getString() {
			return string;
		}
		public void setString(String string) {
			this.string = string;
		}
		public Coo getCoo() {
			return coo;
		}
		public void setCoo(Coo coo) {
			this.coo = coo;
		}
	}
	public static class Coo {
		private long cooLong;

		public long getCooLong() {
			return cooLong;
		}

		public void setCooLong(long cooLong) {
			this.cooLong = cooLong;
		}
	}
	
	@Test
	public void testGenerateInstance() {
		Foo foo = new BeanGenerator<Foo>(Foo.class).generate();
		Assert.assertTrue(foo.getInt() != 0);
		Assert.assertTrue(foo.getBoos().size() > 0);
		Boo boo = (Boo) Atom.getFirstOrThrow(foo.getBoos());
		Assert.assertTrue(boo.getString() != "" && boo.getString() != null);
		Assert.assertTrue(boo.getCoo() != null);
		Assert.assertTrue(boo.getCoo().getCooLong() != 0l);
	}
}

