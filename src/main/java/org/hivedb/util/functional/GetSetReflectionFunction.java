package org.hivedb.util.functional;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.hivedb.util.AccessorFunction;
import org.hivedb.util.classgen.ReflectionTools;

public class GetSetReflectionFunction  {
	
	public static<T> AccessorFunction<Object> CreateFunction(final Class fieldClass, final T instance, final String memberName) {
		return new AccessorFunction<Object>() {			 
			public Object get() {
				return invokeGetter(instance, ReflectionTools.capitalize(memberName));
			}

			public void set(Object value) {
				invokeSetter(instance, ReflectionTools.capitalize(memberName), value);
			}
			public Class getFieldClass() {
				return getGetterMethod(instance, ReflectionTools.capitalize(memberName)).getReturnType();
			}
		};
	}
	private static Object invokeGetter(Object instance, final String memberName)
    {    	
	   try {
		   return getGetterMethod(instance, memberName).invoke(instance, new Object[] {});
	   } catch (Exception exception) {
		   throw new RuntimeException("Exception invoiking method get" + memberName, exception);
	   }
    }
	
	private static Method getGetterMethod(Object instance, final String memberName)  {
		try {
			return instance.getClass().getMethod("get"+memberName, new Class[] {});
		} catch (Exception exception) {
    		throw new RuntimeException("Exception looking up method get" + memberName, exception);
    	}
	}
	
	private static void invokeSetter(Object instance, String memberName, Object value)
    {
		String setterName = "set"+memberName;
    	final String memberNameFinal = memberName;
		// try to match on the value's type
    	try {
    		instance.getClass().getMethod(setterName, new Class[] {value.getClass()})
    			.invoke(instance, new Object[] {value});
    	}
    	catch (Exception exception) {
    		// iterate through all methods until a name-matched setter is found
    		try {
	    		Filter.grepSingle(
    				new Predicate<Method>() {	public boolean f(Method m) {
							return m.getName().equals("set"+memberNameFinal);						
    				}},
    				Arrays.asList(instance.getClass().getMethods()))
	    		.invoke(instance, new Object[] {value});
    		}
	    	catch (Exception e)
	    	{
	    		throw new RuntimeException("Exception calling method set" + memberName 
	    								+ " with a value of type " + value.getClass(), e);
	    	}
    	}
    }
	
}
