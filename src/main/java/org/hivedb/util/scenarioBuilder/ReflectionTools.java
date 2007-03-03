package org.hivedb.util.scenarioBuilder;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

public class ReflectionTools {
	
	// Since Java has dumb getters and setters, this helps match
	// a private field to it's corresponding getter and/or setter
	public static String capitalize(String s) {
	    if (s.length() == 0) return s;
	    return s.substring(0, 1).toUpperCase() + s.substring(1);
	}
	public static Collection<Method> getDeclaredPublicMethods(Class subject) {
		return Filter.grepAgainstList( // get declared in the class and public
			   Arrays.asList(subject.getMethods()),						   
			   Arrays.asList(subject.getDeclaredMethods()));
	}
}
