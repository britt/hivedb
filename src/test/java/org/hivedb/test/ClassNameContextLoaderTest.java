package org.hivedb.test;

import java.util.Arrays;

import junit.framework.Assert;

import org.testng.annotations.Test;

public class ClassNameContextLoaderTest {
	
	@Test
	public void proccesLocationsWithNullArgument() throws Exception {
		ClassNameContextLoader loader = new ClassNameContextLoader();
		String[] locations = loader.processLocations(ClassNameContextLoaderTest.class, null);
		Assert.assertEquals(1, locations.length);
		Assert.assertEquals(ClassNameContextLoaderTest.class.getName() + ".xml", locations[0]);
	}
	
	@Test
	public void proccesLocationsWithEmptyListArgument() throws Exception {
		ClassNameContextLoader loader = new ClassNameContextLoader();
		String[] locations = loader.processLocations(ClassNameContextLoaderTest.class, new String[]{});
		Assert.assertEquals(1, locations.length);
		Assert.assertEquals(ClassNameContextLoaderTest.class.getName() + ".xml", locations[0]);
	}
	
	@Test
	public void proccesLocationsWithArguments() throws Exception {
		ClassNameContextLoader loader = new ClassNameContextLoader();
		String[] locations = loader.processLocations(ClassNameContextLoaderTest.class, new String[]{"another.xml"});
		Assert.assertEquals(2, locations.length);
		Assert.assertTrue(Arrays.asList(locations).contains("another.xml"));
		Assert.assertTrue(Arrays.asList(locations).contains(ClassNameContextLoaderTest.class.getName() + ".xml"));
	}
}
