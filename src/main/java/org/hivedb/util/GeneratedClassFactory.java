package org.hivedb.util;

import java.util.Map;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;


public class GeneratedClassFactory {

	private static MethodInterceptor getDefaultInterceptor(Class clazz) {
		return new GeneratedInstanceInterceptor(clazz);
	}
	
	public static<T> Class<? extends T> getGeneratedClass(final Class<T> clazz, MethodInterceptor interceptor) {
		// Only generate for interfaces.
		// Implementations can add whatever functionality they need, and so don't warrant a generated class
		if (!clazz.isInterface())	
			return clazz;
		Enhancer e = new Enhancer();
		e.setCallbackType(interceptor.getClass());
		e.setNamingPolicy(new ImplNamer(clazz));
		e.setSuperclass(Mapper.class);
		e.setInterfaces(new Class[] {clazz, PropertyAccessor.class, GeneratedImplementation.class});
		Class<? extends T> generatedClass = e.createClass();
		Enhancer.registerCallbacks(generatedClass, new Callback[] {interceptor});
		return generatedClass;
	}
	public static<T> Class<? extends T> getGeneratedClass(final Class<T> clazz) {
		return getGeneratedClass(clazz, getDefaultInterceptor(clazz));
	}
	
	public static<T> T newInstance( Class<T> clazz, MethodInterceptor interceptor ){
		try{
			return getGeneratedClass(clazz, interceptor).newInstance();
		}catch( Throwable e ){
			 e.printStackTrace();
			 throw new RuntimeException(e.getMessage(), e);
		}
	}
	public static<T> T newInstance( Class<T> clazz ){
		return newInstance(clazz, getDefaultInterceptor(clazz));
	}
	
	public static<T> T newInstance( Class<T> clazz, MethodInterceptor interceptor, Map<String, Object> prototype ){
		PropertyAccessor instance = (PropertyAccessor) newInstance(clazz, interceptor);
		for (String propertyName : ReflectionTools.getPropertiesOfGetters((Class<?>)clazz))
			instance.set(propertyName, prototype.get(propertyName));
		return (T) instance;
	}
	public static<T> T newInstance( Class<T> clazz, Map<String, Object> prototype ){
		return newInstance(clazz, getDefaultInterceptor(clazz), prototype);
	}
}
