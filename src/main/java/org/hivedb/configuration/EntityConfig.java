package org.hivedb.configuration;

import java.io.Serializable;
import java.util.Collection;


public interface EntityConfig {
	
	public enum IndexType {
		HiveIndex,
		DataIndex
	}
	String getPrimaryIndexKeyPropertyName();
	String getPartitionDimensionName();
	Object getPrimaryIndexKey(Object instance);
	
	String getIdPropertyName();
	Serializable getId(Object instance);

	// TODO remove in favor of getIndexConfigs
	Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs();
	
	Collection<? extends EntityIndexConfig> getIndexConfigs(IndexType indexType);
	String getResourceName();
	
	boolean isPartitioningResource();
	Class<?> getRepresentedInterface();
	
	Class<?> getPrimaryKeyClass();
	Class<?> getIdClass();
	
	String getStoredVersionProperty();
}
