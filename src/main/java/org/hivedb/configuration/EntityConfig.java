package org.hivedb.configuration;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.annotations.IndexType;


public interface EntityConfig {
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	String getIdPropertyName();
	Serializable getId(Object instance);

	Collection<? extends EntityIndexConfig> getEntityIndexConfigs();
	EntityIndexConfig getEntityIndexConfig(String propertyName);
	
	Collection<? extends EntityIndexConfig> getEntityIndexConfigs(IndexType indexType);
	String getResourceName();
	
	boolean isPartitioningResource();
	Class<?> getRepresentedInterface();
	
	Class<?> getPrimaryKeyClass();
	Class<?> getIdClass();	
	
	int getVersion(Object instance);
}
