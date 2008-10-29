package org.hivedb.hibernate.simplified.configuration;

import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.entity.EntityIndexConfig;

import java.util.Collection;
import java.util.EnumSet;


public interface EntityConfig<T> {
	String getPartitionKeyPropertyName();
	String getPartitionDimensionName();
  EntityIndexConfig getPartitionEntityIndexConfig();
  Class<?> getPartitionKeyClass();  
	String getIdPropertyName();
	Collection<EntityIndexConfig> getEntityIndexConfigs();
	EntityIndexConfig getEntityIndexConfig(String propertyName);
	Collection<EntityIndexConfig> getEntityIndexConfigs(EnumSet<IndexType> indexTypes);
	Collection<EntityIndexConfig> getEntityIndexConfigs(IndexType indexType);
  String getResourceName();
	boolean isPartitioningResource();
	Class<T> getEntityClass();
	Class<?> getIdClass();
}
