package org.hivedb.configuration.entity;

import org.hivedb.configuration.entity.PartitionDimensionConfig;

import java.util.Collection;

public interface EntityHiveConfig extends PartitionDimensionConfig {
	public Collection<EntityConfig> getEntityConfigs();
	public EntityConfig getEntityConfig(Class<?> clazz);
	public EntityConfig getEntityConfig(String clazz);
}
