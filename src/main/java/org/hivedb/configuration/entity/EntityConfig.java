package org.hivedb.configuration.entity;

import org.hivedb.annotations.IndexType;

import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;


public interface EntityConfig {
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	String getIdPropertyName();
	Serializable getId(Object instance);

	Collection<EntityIndexConfig> getEntityIndexConfigs();
	EntityIndexConfig getEntityIndexConfig(String propertyName);
	EntityIndexConfig getPrimaryIndexKeyEntityIndexConfig();
	
	Collection<EntityIndexConfig> getEntityIndexConfigs(EnumSet<IndexType> indexTypes);
	Collection<EntityIndexConfig> getEntityIndexConfigs(IndexType indexType);
	String getResourceName();
	
	boolean isPartitioningResource();
	Class<?> getRepresentedInterface();
	
	Class<?> getPrimaryKeyClass();
	Class<?> getIdClass();	

  // TODO Remove
  @Deprecated
  int getVersion(Object instance);

	/**
	 * Get classes that are properties of this class, singletons or collections, which have a property indexed by the hive
	 * @return
	 */
  // TODO remove, this class shouldn't know about this
  Collection<Class<?>> getAssociatedClasses();
}
