package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.annotations.IndexType;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.InstanceCollectionValueGetter;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.SqlUtils;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Validator;

public class EntityIndexConfigProxy extends EntityIndexConfigImpl implements EntityIndexConfigDelegator {
		
	private EntityConfig delegateEntityConfig;
	/**
	 *  Constructor for a primitive type property or collection or primitive types
	 * @param secondaryIndexKeyPropertyName
	 * @param isManyToMultiplicity - is the property a collection
	 */
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
			final String innerClassPropertyName) {
		super(entityInterface, propertyName, innerClassPropertyName);
	}

	public IndexType getIndexType() {
		return IndexType.HiveForeignKey;
	}
	public EntityConfig getDelegateEntityConfig() {
		return delegateEntityConfig;
	}
	public Object stubEntityInstance(Object entityId, Object primaryIndexKey) {
		Object instance = GeneratedInstanceInterceptor.newInstance(this.getDelegateEntityConfig().getRepresentedInterface());
		GeneratedInstanceInterceptor.setProperty(instance, this.getDelegateEntityConfig().getIdPropertyName(), entityId);
		GeneratedInstanceInterceptor.setProperty(instance, this.getDelegateEntityConfig().getPrimaryIndexKeyPropertyName(), primaryIndexKey);
		return instance;
	}
}
