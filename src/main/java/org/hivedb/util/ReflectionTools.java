package org.hivedb.util;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

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
	public static boolean doesImplement(Class doesClass, Class implementThisInterface)
	{
		return doesClass.equals(implementThisInterface) ||
			doesImplement(doesClass.getInterfaces(), implementThisInterface) ||
				(doesClass.getSuperclass() != null &&
				!doesClass.getSuperclass().equals(Object.class) &&	
				 doesImplement(doesClass.getSuperclass(), implementThisInterface));
		 
	}
	public static boolean doesImplement(final Class[] doesOneOfThese, final Class matchOrImplementThisInterface)
	{
		return Filter.isMatch(new Predicate<Class>() {
			
			public boolean f(Class anInterface) {
				return anInterface.equals(matchOrImplementThisInterface) || 
						doesImplement(anInterface.getInterfaces(), matchOrImplementThisInterface);
					
			}},
			doesOneOfThese);
	}
	/**
	 *  Returns the interface in the list that is mostly closest to the given class/interface.
	 *  The order of search is 1) see if the class matches one of the given interfaces,
	 *  2) see if one of the class's implemented interfaces and interfaces of those interfaces implements
	 *  one of the given interfaces, 3) rerun this method on the given class's parent class.
	 *  
	 *  This test should be used when there is one obvious answer, not when the answer may be ambiguous,
	 *  in which case the returned class will not be meaningful.
	 * @param doesClassOrInterface
	 * @param implementOneOfTheseInterfaces
	 * @return
	 */
	public static Class whichIsImplemented(final Class doesClassOrInterface, final Collection<Class> implementOneOfTheseInterfaces)
	{
		Class answer = Filter.grepSingleOrNull(new Predicate<Class>() {
			public boolean f(Class implementThisInterface) {
				return doesClassOrInterface.equals(implementThisInterface);
			}},
			implementOneOfTheseInterfaces);
		if (answer != null)
			return answer;
		
		answer = Filter.grepSingleOrNull(new Filter.NotNullPredicate<Class>(),
				Transform.map(new Unary<Class, Class>() {
					public Class f(Class anInterface) {
						return whichIsImplemented(anInterface, implementOneOfTheseInterfaces);
					}},
					Arrays.asList(doesClassOrInterface.getInterfaces())));
		if (answer != null)
			return answer;
		
		return  
			(doesClassOrInterface.getSuperclass() != null && !doesClassOrInterface.getSuperclass().equals(Object.class))
					? whichIsImplemented(doesClassOrInterface.getSuperclass(), implementOneOfTheseInterfaces)
					: null;
	}
	
	/**
	 *  Call the default constructor
	 * @param <T>
	 * @param type
	 * @param partitionDimensionName
	 * @return
	 */
	public static<T> T carefreeConstructor(Class<T> type) {
		try {
			return type.getConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}
	/**
	 *  Call a one-argument constructor
	 * @param <T>
	 * @param type
	 * @param partitionDimensionName
	 * @param argument
	 * @return
	 */
	public static<T> T carefreeConstructor(Class<T> type, String partitionDimensionName, Object argument) {
		try {
			return type.getConstructor(new Class[] {argument.getClass()}).newInstance(new Object[] {argument});
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}
}
