package org.hivedb.configuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;

import org.hivedb.annotations.IndexType;


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
	
	int getVersion(Object instance);

	/**
	 * Get classes that are properties of this class, singletons or collections, which have a property indexed by the hive
	 * @return
	 */
	Collection<Class<?>> getAssociatedClasses();
}
