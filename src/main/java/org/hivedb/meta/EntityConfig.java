package org.hivedb.meta;

import java.util.Collection;

public interface EntityConfig<F extends Object> {
	
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	public String getIdPropertyName();
	public F getId(Object instance);

	Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs();
	String getResourceName();
	
	boolean isPartitioningResource();
	Class getRepresentedInterface();
	Class getIdClass();
}
