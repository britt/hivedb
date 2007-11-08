package org.hivedb.configuration;

import java.util.Collection;


public interface EntityConfig {
	
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	public String getIdPropertyName();
	public Object getId(Object instance);

	Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs();
	String getResourceName();
	
	boolean isPartitioningResource();
	Class<?> getRepresentedInterface();
	Class<?> getIdClass();
}
