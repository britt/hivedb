package org.hivedb.configuration.entity;

import org.hivedb.annotations.IndexType;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;

public class EntityIndexConfigProxy extends EntityIndexConfigImpl implements EntityIndexConfigDelegator {
		
	private EntityConfig delegateEntityConfig;
	/**
	 *  Constructor for a primitive type property or collection or primitive types
	 * @param secondaryIndexKeyPropertyName
	 * @param isManyToMultiplicity - is the property a collection
	 */
	@SuppressWarnings("unchecked")
	public EntityIndexConfigProxy(
			final Class entityInterface,
			final String propertyName,
			final EntityConfig delegateEntityConfig) {
		super(entityInterface, propertyName);
		this.delegateEntityConfig = delegateEntityConfig;
	}
	/**
	 *  Constructor for a property of a collection of complex types. This assumes the complex type's
	 *  indexed property is a primitive value, though this could be refactored to support a property
	 *  at any depth in the object graph. 
	 *  
	 * @param propertyName name of the collection property
	 * @param innerClassPropertyName name of the property of the collection item class
	 */
	public EntityIndexConfigProxy(
			final Class<?> entityInterface,
			final String propertyName,
			final String innerClassPropertyName,
			final EntityConfig delegateEntityConfig) {
		super(entityInterface, propertyName, innerClassPropertyName);
		this.delegateEntityConfig = delegateEntityConfig;
	}

	public IndexType getIndexType() {
		return IndexType.Delegates;
	}
	public EntityConfig getDelegateEntityConfig() {
		return delegateEntityConfig;
	}
	public Object stubEntityInstance(Object entityId, Object primaryIndexKey) {
		Object instance = GeneratedClassFactory.newInstance(this.getDelegateEntityConfig().getRepresentedInterface());
		GeneratedInstanceInterceptor.setProperty(instance, this.getDelegateEntityConfig().getIdPropertyName(), entityId);
		GeneratedInstanceInterceptor.setProperty(instance, this.getDelegateEntityConfig().getPrimaryIndexKeyPropertyName(), primaryIndexKey);
		return instance;
	}
}
