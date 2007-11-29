package org.hivedb.util;

import java.lang.reflect.Method;
import java.util.Collection;

import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;

public class BeanGenerator<R> implements Generator<R> {
	private static final int COLLECTION_SIZE = 3;
	private Class<R> clazz = null;
	
	public BeanGenerator(Class<R> clazz) { this.clazz = clazz;} 
	
	public R generate() {
//		validateBean();
		R instance;
		try {
			instance = clazz.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return randomlyPopulateProperties(instance);
	}
	
	@SuppressWarnings("unchecked")
	private R randomlyPopulateProperties(R instance) {
		for (Method getter : ReflectionTools.getGetters(clazz))
	    {
	    	if (getter.getDeclaringClass().equals(Object.class))
	    		continue; // Only interfaces should be reflected upon here, until then we skip Object methods
	    	String propertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
	    	Class<Object> propertyClazz = (Class<Object>) getter.getReturnType();
			if (ReflectionTools.doesImplementOrExtend(propertyClazz, Collection.class)) {
	    		Class<Object> collectionItemClass = (Class<Object>) ReflectionTools.getCollectionItemType(clazz,propertyName);
	    		setCollection(instance, propertyName, collectionItemClass);
	    	} else
				setProperty(instance, propertyName, propertyClazz);
	    }
		return instance;
	}

	private void setProperty(R instance, String propertyName, Class<Object> clazz) {
		ReflectionTools.invokeSetter(instance, propertyName, 
				PrimitiveUtils.isPrimitiveClass(clazz)
					? new GeneratePrimitiveValue<Object>(clazz).generate()
					: new BeanGenerator<Object>(clazz).generate());
		
	}

	private void setCollection(R instance, String propertyName,
			Class<Object> collectionItemClass) {
		ReflectionTools.invokeSetter(instance, propertyName, 
				PrimitiveUtils.isPrimitiveClass(collectionItemClass)
					? new GeneratePrimitiveCollection<Object>(collectionItemClass,COLLECTION_SIZE).generate()
					: generateObjectCollection(collectionItemClass, COLLECTION_SIZE));
//					: new GenerateInstanceCollection<Object>(collectionItemClass, COLLECTION_SIZE).generate());
	}

	private Collection<?> generateObjectCollection(Class<?> collectionItemClass, int size) {
		return Generate.create(new BeanGenerator(collectionItemClass), new NumberIterator(size));
	}
	
	private void validateBean() {
		try {
			if(this.clazz.getConstructor(new Class[]{}) == null)
				throw new RuntimeException(
						String.format(
							"%s cannot be instantiated, it must have a publicly accessible constructor that takes no arguments", clazz));
		} catch (SecurityException e) {
			throw new RuntimeException(
				String.format(
					"%s cannot be instantiated, it must have a publicly accessible constructor that takes no arguments", clazz),
					e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(
				String.format(
					"%s cannot be instantiated, it must have a publicly accessible constructor that takes no arguments", clazz),
					e);
		}
	}

}
