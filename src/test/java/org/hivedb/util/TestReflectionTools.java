package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.hivedb.util.ReflectionTools;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestReflectionTools  {
	@Test
	public void testDoesImplement() {
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(new Fooimpimp(){}.getClass(), Coo.class));
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
	private class Fooimp implements Coo {

		public Collection<Boo> getBoos() {
			return null;
		}

		public Float getFloo() {
			return null;
		}}
	private class Fooimpimp extends Fooimp {

		public Collection<Coo> getCoos() {
			return null;
		}

		public Integer getIoo() {
			// TODO Auto-generated method stub
			return null;
		}

		public Collection<Short> getShoos() {
			return null;
		}

		public String getStroo() {
			return null;
		}}
}
