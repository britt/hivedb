package org.hivedb.configuration;

import java.util.Collection;


public interface EntityConfig {
	
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	String getIdPropertyName();
	Object getId(Object instance);

	Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs();
	String getResourceName();
	
	boolean isPartitioningResource();
	Class<?> getRepresentedInterface();
	
	Class<?> getPrimaryKeyClass();
	Class<?> getIdClass();
}
