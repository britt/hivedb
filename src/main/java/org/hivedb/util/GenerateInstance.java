package org.hivedb.util;

import java.lang.reflect.Method;
import java.util.Collection;


import org.hivedb.util.functional.Generator;

public class GenerateInstance<T> implements Generator<T> {
	
	private final int COLLECTION_SIZE = 3;
	private Class<T> interfaceToImplement;
	public GenerateInstance(Class<T> interfaceToImplement)
	{
		this.interfaceToImplement = interfaceToImplement;
	}
	
	public T generateAndCopyProperties(Object templateInstance) {
		T instance = generate();
		for (String propertyName : ReflectionTools.getPropertiesOfGetters(interfaceToImplement))
			ReflectionTools.invokeSetter(
				instance, 
				propertyName, 
				ReflectionTools.invokeGetter(templateInstance, propertyName));
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	public T generate() {
		T instance = (T)Interceptor.newInstance( interfaceToImplement );
		    
	    for (Method getter : ReflectionTools.getGetters(interfaceToImplement))
	    {
	    	if (getter.getDeclaringClass().equals(Object.class))
	    		continue; // Only interfaces should be reflected upon here, until then we skip Object methods
	    	String propertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
	    	Class<Object> clazz = (Class<Object>) getter.getReturnType();
			if (ReflectionTools.doesImplementOrExtend(clazz, Collection.class)) {
	    		Class<Object> collectionItemClass = (Class<Object>) ReflectionTools.getCollectionItemType(interfaceToImplement,propertyName);
	    		((PropertySetter<T>)instance).set(propertyName,
	    				PrimitiveUtils.isPrimitiveClass(collectionItemClass)
	    					? new GeneratePrimitiveCollection<Object>(collectionItemClass,COLLECTION_SIZE).generate()
	    					: new GenerateInstanceCollection<Object>(collectionItemClass, COLLECTION_SIZE).generate());
	    	}
	    	else 
	    		((PropertySetter<T>)instance).set(propertyName,
	    				PrimitiveUtils.isPrimitiveClass(clazz)
	    					? new GeneratePrimitiveValue<Object>(clazz).generate()
	    					: new GenerateInstance<Object>(clazz).generate());
	    }
	    return instance;
	}
}
