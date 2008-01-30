package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.IndexType;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Validate;
import org.hivedb.util.InstanceCollectionValueGetter;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.SqlUtils;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.validators.NonNullValidator;
import org.hivedb.util.validators.Validator;

public class EntityIndexConfigImpl implements EntityIndexConfig {
		
	/**
	 *  Constructor for a primitive type property or collection or primitive types
	 */
	public EntityIndexConfigImpl(
			final Class entityInterface,
			final String propertyName) {
		this.propertyName = propertyName;
		this.indexName = propertyName;
		this.indexType = resolveIndexType(entityInterface, propertyName);
		this.secondaryIndexCollectionGetter = new InstanceCollectionValueGetter() {
			public Collection<Object> get(Object instance) {
				if (!getValidator().isValid(instance, propertyName))
					getValidator().throwInvalid(instance, propertyName);
				return Actor.forceCollection(ReflectionTools.invokeGetter(instance, propertyName));
		}};
		this.indexClass = ReflectionTools.getPropertyTypeOrPropertyCollectionItemType(entityInterface, propertyName);
		this.validator = resolveValidator(entityInterface, propertyName);
	}
	
	/**
	 *  Constructor for a property of a collection of complex types. This assumes the complex type's
	 *  indexed property is a primitive value, though this could be refactored to support a property
	 *  at any depth in the object graph. 
	 *  
	 * @param propertyName name of the collection property
	 * @param innerClassPropertyName name of the property of the collection item class
	 */
	public EntityIndexConfigImpl(
			final Class<?> entityInterface,
			final String propertyName,
			final String innerClassPropertyName) {
		this.propertyName = propertyName;
		this.innerClassPropertyName = innerClassPropertyName;
		this.indexName = SqlUtils.singularize(propertyName) + ReflectionTools.capitalize(innerClassPropertyName); 
		this.indexType = ReflectionTools.getGetterOfProperty(entityInterface, propertyName).getAnnotation(Index.class).type();
		this.secondaryIndexCollectionGetter = new InstanceCollectionValueGetter() {
			public Collection<Object> get(Object instance) {
				if (!getValidator().isValid(instance, propertyName))
					getValidator().throwInvalid(instance, propertyName);
				return Transform.map(new Unary<Object, Object>() {
					public Object f(Object collectionItem) {
						return ReflectionTools.invokeGetter(collectionItem, innerClassPropertyName);
				}},
				(Collection<Object>)ReflectionTools.invokeGetter(instance, propertyName));					
		}};
		this.indexClass = ReflectionTools.getPropertyType(
			ReflectionTools.getCollectionItemType(entityInterface, propertyName),
			innerClassPropertyName);
		this.validator = resolveValidator(entityInterface, propertyName);
	}
	
	private IndexType resolveIndexType(final Class entityInterface, final String propertyName) {
		Index annotation = ReflectionTools.getGetterOfProperty(entityInterface, propertyName).getAnnotation(Index.class);
		if (annotation != null)
			return annotation.type();
		if (ReflectionTools.getGetterOfProperty(entityInterface, propertyName).getAnnotation(PartitionIndex.class) != null)
			return IndexType.Partition;
		return IndexType.None;
	}
	
	private Validator resolveValidator(final Class entityInterface, final String propertyName) {
		Validate annotation = (Validate) ReflectionTools.getGetterOfProperty(entityInterface, propertyName).getAnnotation(Validate.class);
		if (annotation != null)
			try {
				return (Validator) Class.forName(annotation.value()).newInstance();
			} catch (Exception e) {
				throw new HiveRuntimeException(e);
			}
		return nonNullValidator;
	}
	private static Validator nonNullValidator = new NonNullValidator();
	

	private String indexName;
	public String getIndexName() {
		return indexName;
	}
	private String propertyName;
	public String getPropertyName() {
		return propertyName;
	}
	
	private String innerClassPropertyName;
	public String getInnerClassPropertyName() {
		if (innerClassPropertyName == null)
			throw new RuntimeException(String.format("EntityIndexConfig's property %s is not a collection property, but is being treated like one", propertyName));
		return innerClassPropertyName;
	}
	
	private InstanceCollectionValueGetter secondaryIndexCollectionGetter;
	public Collection<Object> getIndexValues(Object instance) {
		return secondaryIndexCollectionGetter.get(instance);
	}

	private Class<?> indexClass;
	public Class<?> getIndexClass() {
		return indexClass;
	}
	private IndexType indexType;
	public IndexType getIndexType() {
		return indexType;
	}
	private Validator validator;
	public Validator getValidator() {
		return validator;
	}
	
}
