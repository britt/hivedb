package org.hivedb.util;

import java.lang.reflect.Method;


public class AccessorReflectionFunction<T,F>  {
	 
	public AccessorFunction<F> create(final Class<? extends F> fieldClass, final T instance, final String memberName) {
		return new AccessorFunction<F>() {			 
			public F get() {
				return invokeGetter(instance, ReflectionTools.capitalize(memberName));
			}

			public void set(F value) {
				ReflectionTools.invokeSetter(instance, memberName, value);
			}
			public Class getFieldClass() {
				return fieldClass;
			}
		};
	}
	
	public AccessorFunction<F> createForConstant(final Class<? extends F> fieldClass, final F value) {
		return new AccessorFunction<F>() {			 
			public F get() {
				return value;
			}
			public void set(F value) { }
			public Class getFieldClass() {
				return fieldClass;
			}
		};
	}
	
	@SuppressWarnings("unchecked")
	private F invokeGetter(T instance, final String memberName)
    {    	
	   try {
		   return (F) getGetterMethod(instance, memberName).invoke(instance, new Object[] {});
	   } catch (Exception exception) {
		   throw new RuntimeException("Exception invoiking method get" + memberName, exception);
	   }
    }
	
	private Method getGetterMethod(Object instance, final String memberName)  {
		try {
			return instance.getClass().getMethod("get"+memberName, new Class[] {});
		} catch (Exception exception) {
    		throw new RuntimeException("Exception looking up method get" + memberName, exception);
    	}
	}
	
	
}
