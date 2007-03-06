package org.hivedb.util;

import junit.framework.Assert;

import org.hivedb.util.scenarioBuilder.MaximumInvocationCountMatcher;
import org.jmock.core.InvocationMatcher;

public class TestUtils {
	public static InvocationMatcher noMoreThan(int times){
		return new MaximumInvocationCountMatcher(times);
	}
	
	public static void assertImplements(Class expected, Object actual) {
		Class[] implemented = actual.getClass().getInterfaces();
		boolean implementsInterface = false;
		for(Class inter : implemented)
			implementsInterface |= expected.equals(inter);
		Assert.assertTrue("Object did not implement " + expected.toString(), implementsInterface);
	}
}
