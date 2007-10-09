package org.hivedb.util;

import java.lang.reflect.Method;


public class PropertiesReflectionAccessor implements PropertiesAccessor {
	 
	Class<?> representedInterface;
	public PropertiesReflectionAccessor(Class<?> representedInterface) {
		this.representedInterface = representedInterface;
	}
		 
	public Object get(final String propertyName, final Object instance) {
		return invokeGetter(instance, ReflectionTools.capitalize(propertyName));
	}

	public void set(final String propertyName, final Object instance, final Object value) {
		ReflectionTools.invokeSetter(instance, propertyName, value);
	}
	public Class getFieldClass(final String propertyName) {
		return ReflectionTools.getPropertyType(representedInterface, propertyName);
	}
	
	public static<F> AccessorFunction<F> createForConstant(final Class<? extends F> fieldClass, final F value) {
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
	private static<T,F> F invokeGetter(T instance, final String memberName)
    {    	
	   try {
		   return (F) getGetterMethod(instance, memberName).invoke(instance, new Object[] {});
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
	
	
}
