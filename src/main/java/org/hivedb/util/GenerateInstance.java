package org.hivedb.util;

import java.lang.reflect.Method;
import java.util.Collection;


import org.hivedb.annotations.Ignore;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;

public class GenerateInstance<T> implements Generator<T> {
	
	private final int COLLECTION_SIZE = 3;
	private Class<T> clazz;
	public GenerateInstance(Class<T> clazz)
	{
		this.clazz = clazz;
	}
	
	public T generateAndCopyProperties(Object templateInstance) {
		T instance = generate();
		for( Method getter : ReflectionTools.getGetters(clazz)) {
			
			if (getter.getDeclaringClass().equals(Object.class))
	    		continue;
			else {
				String propertyName = BeanUtils.findPropertyForMethod(getter).getName();
				Object value = ReflectionTools.invokeGetter(templateInstance, propertyName);
				if (ReflectionTools.isCollectionProperty(clazz, propertyName)) {
					Collection<Object> collection = (Collection<Object>)value;
					final GenerateInstance<Object> generateInstance = new GenerateInstance<Object>((Class<Object>) Atom.getClassFirstOrDefault(collection, Object.class));
					GeneratedInstanceInterceptor.setProperty(
						instance,
						propertyName,
						Transform.map(new Unary<Object,Object>() {
							public Object f(Object item) {
								return generateInstance.generateAndCopyProperties(item);
							}}, collection));
				}
				else
					GeneratedInstanceInterceptor.setProperty(
						instance, 
						propertyName, 
						value);
			}
			
		}
		return instance;
	}
	
	static QuickCache primitiveGenerators = new QuickCache(); // cache generators for sequential randomness
	@SuppressWarnings("unchecked")
	public T generate() {
		T instance;
		try {
			instance = clazz.isInterface() 
				? (T)GeneratedInstanceInterceptor.newInstance( clazz )
				: clazz.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
			
	    for (Method getter : ReflectionTools.getGetters(clazz))
	    {
	    	if (getter.getDeclaringClass().equals(Object.class))
	    		continue;
	    	String propertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
	    	Class methodOwner = ReflectionTools.getOwnerOfMethod(clazz, propertyName);
	    	Method method = ReflectionTools.getGetterOfProperty(methodOwner, propertyName);
	    	
	    	if (method.getAnnotation(Ignore.class) != null)
	    		continue; 
	    	
	    	final Class<Object> returnType = (Class<Object>) getter.getReturnType();
			if (ReflectionTools.doesImplementOrExtend(returnType, Collection.class)) {
	    		Class<Object> collectionItemClass = (Class<Object>) ReflectionTools.getCollectionItemType(clazz,propertyName);
	    		ReflectionTools.invokeSetter(instance, propertyName,
	    				PrimitiveUtils.isPrimitiveClass(collectionItemClass)
	    					? new GeneratePrimitiveCollection<Object>(collectionItemClass,COLLECTION_SIZE).generate()
	    					: new GenerateInstanceCollection<Object>(collectionItemClass, COLLECTION_SIZE).generate());
	    	} else if(returnType.isArray()) {
	    		Class<Object> arrayItemClass = (Class<Object>) returnType.getComponentType();
	    		ReflectionTools.invokeSetter(instance, propertyName,
	    				PrimitiveUtils.isPrimitiveClass(arrayItemClass)
	    					? new GeneratePrimitiveCollection<Object>(arrayItemClass,COLLECTION_SIZE).generate().toArray()
	    					: new GenerateInstanceCollection<Object>(arrayItemClass, COLLECTION_SIZE).generate().toArray());
	    	}
	    	else 
	    		ReflectionTools.invokeSetter(instance, propertyName,
	    				PrimitiveUtils.isPrimitiveClass(returnType)
	    					? primitiveGenerators.get(returnType.hashCode(), new Delay<Generator>() {
	    						public Generator f() {
	    							return new GeneratePrimitiveValue<Object>(returnType);
	    						}}).generate()
	    					: new GenerateInstance<Object>(returnType).generate());
	    }
	    return instance;
	}
}
