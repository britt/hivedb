package org.hivedb.meta;

import java.util.Collection;

public interface EntityConfig<F extends Object> {
	
	String getPrimaryIndexKeyPropertyName();
	//TODO Remove this method
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	public String getIdPropertyName();
	//TODO consider removing this generic
	public F getId(Object instance);

	Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs();
	String getResourceName();
	
	boolean isPartitioningResource();
	Class getRepresentedInterface();
	Class getIdClass();
}
