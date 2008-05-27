package org.hivedb.util.classgen;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import net.sf.cglib.proxy.Factory;

import org.hivedb.annotations.GeneratorIgnore;
import org.hivedb.util.Lists;
import org.hivedb.util.MapBacked;
import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.QuickCache;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;


public class GenerateInstance<T> implements Generator<T> {
	private final int COLLECTION_SIZE = 3;
	private Class<T> clazz;
	private Collection<Class<?>> excludedClasses = defaultExcludedClasses();
	
	public GenerateInstance(Class<T> clazz)
	{
		this.clazz = clazz;
	}
	
	public T generateAndCopyProperties(Object templateInstance) {
		if (PrimitiveUtils.isPrimitiveClass(clazz))
			throw new RuntimeException(String.format("Attempt to generate instance and copy properties of a primitive class: %s", clazz.getName()));
		T instance = newInstance();
		for( Method getter : ReflectionTools.getGetters(clazz)) {
			
			if (belongsToExcludedClass(getter))
	    		continue;
			else {
				String propertyName = BeanUtils.findPropertyForMethod(getter).getName();
				final Object value = ReflectionTools.invokeGetter(templateInstance, propertyName);
				
				if (ReflectionTools.isCollectionProperty(clazz, propertyName)) {
					Collection<Object> collection = value == null ? Lists.newArrayList() : (Collection<Object>)value;
					
					final Class<?> itemClass = ReflectionTools.getCollectionItemType(clazz, propertyName);
					final GenerateInstance<Object> generateInstance = new GenerateInstance<Object>((Class<Object>) itemClass);
					GeneratedInstanceInterceptor.setProperty(
						instance,
						propertyName,
						Transform.map(new Unary<Object,Object>() {
							public Object f(Object item) {
								return PrimitiveUtils.isPrimitiveClass(itemClass)
								 	? item
									: generateInstance.generateAndCopyProperties(item);
							}}, collection));
				}
				else if(value == null)
					continue;
				else if(PrimitiveUtils.isPrimitiveClass(value.getClass()))
					GeneratedInstanceInterceptor.setProperty(
						instance, 
						propertyName, 
						value);
				else
					GeneratedInstanceInterceptor.setProperty(
						instance, 
						propertyName, 
						new GenerateInstance<Object>((Class<Object>) getter.getReturnType()).generateAndCopyProperties(value));
			}
			
		}
		return instance;
	}
	
	static QuickCache primitiveGenerators = new QuickCache(); // cache generators for sequential randomness
	@SuppressWarnings("unchecked")
	public T generate() {
		if (PrimitiveUtils.isPrimitiveClass(clazz))
			return new GeneratePrimitiveValue<T>(clazz).generate();
		T instance = newInstance();
			
	    for (Method getter : ReflectionTools.getGetters(clazz))
	    {
	    	if (belongsToExcludedClass(getter))
	    		continue;
	    	String propertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
	    	Class methodOwner = ReflectionTools.getOwnerOfMethod(clazz, propertyName);
	    	Method method =  ReflectionTools.getGetterOfProperty(methodOwner, propertyName);

	    	if (ReflectionTools.getMethodOfOwner(method).getAnnotation(GeneratorIgnore.class) != null)
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

	private T newInstance() {
		T instance;
		try {
			instance = clazz.isInterface() || ReflectionTools.doesImplementOrExtend(clazz, GeneratedImplementation.class)
				? (T)GeneratedClassFactory.newInstance( clazz )
				: clazz.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	public boolean belongsToExcludedClass(Method m) {
		return Filter.grepItemAgainstList(ReflectionTools.getMethodOfOwner(m).getDeclaringClass(), excludedClasses);
	}
	
	public static Collection<Class<?>> defaultExcludedClasses() {
		return Arrays.asList(new Class<?>[]{Object.class, Factory.class, MapBacked.class});
	}
}
