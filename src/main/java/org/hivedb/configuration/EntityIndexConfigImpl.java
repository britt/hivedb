package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.util.InstanceCollectionValueGetter;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class EntityIndexConfigImpl implements EntityIndexConfig {
		
	/**
	 *  Constructor for a primitive type property or collection or primitive types
	 * @param secondaryIndexKeyPropertyName
	 * @param isManyToMultiplicity - is the property a collection
	 */
	public EntityIndexConfigImpl(
			final Class entityInterface,
			final String secondaryIndexKeyPropertyName) {
		this.secondaryIndexKeyPropertyName = secondaryIndexKeyPropertyName;
		this.secondaryIndexCollectionGetter = new InstanceCollectionValueGetter() {
			public Collection<Object> get(Object instance) {
				return Actor.forceCollection(ReflectionTools.invokeGetter(instance, secondaryIndexKeyPropertyName));
		}};
		this.secondaryIndexClass = ReflectionTools.getPropertyTypeOrPropertyCollectionItemType(entityInterface, secondaryIndexKeyPropertyName);
	}
	/**
	 *  Constructor for a property of a collection of complex types. This assumes the complex type's
	 *  indexed property is a primitive value, though this could be refactored to support a property
	 *  at any depth in the object graph. 
	 *  
	 * @param secondaryIndexKeyPropertyName name of the collection property
	 * @param collectionItemPropertyName name of the property of the collection item class
	 */
	public EntityIndexConfigImpl(
			final Class<?> entityInterface,
			final String secondaryIndexKeyPropertyName,
			final String collectionItemPropertyName) {
		this.secondaryIndexKeyPropertyName = secondaryIndexKeyPropertyName;
		this.secondaryIndexCollectionGetter = new InstanceCollectionValueGetter() {
			public Collection<Object> get(Object instance) {
				return Transform.map(new Unary<Object, Object>() {
					public Object f(Object collectionItem) {
						return ReflectionTools.invokeGetter(collectionItem, collectionItemPropertyName);
				}},
				(Collection<Object>)ReflectionTools.invokeGetter(instance, secondaryIndexKeyPropertyName));					
		}};
		this.secondaryIndexClass = ReflectionTools.getPropertyType(
			ReflectionTools.getCollectionItemType(entityInterface, secondaryIndexKeyPropertyName),
			collectionItemPropertyName);
	}
	
	private String secondaryIndexKeyPropertyName;
	public String getIndexName() {
		return secondaryIndexKeyPropertyName;
	}
	private InstanceCollectionValueGetter secondaryIndexCollectionGetter;
	public Collection<Object> getIndexValues(Object instance) {
		return secondaryIndexCollectionGetter.get(instance);
	}

	private Class<?> secondaryIndexClass;
	public Class<?> getIndexClass() {
		return secondaryIndexClass;
	}
}
