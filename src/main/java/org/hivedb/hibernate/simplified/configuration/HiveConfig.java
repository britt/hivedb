package org.hivedb.hibernate.simplified.configuration;

import org.hivedb.configuration.entity.EntityConfig;

import java.util.Collection;

public interface HiveConfig {
	public String getPartitionDimensionName();
	public Class<?> getPartitionDimensionType();
	public Collection<EntityConfig> getEntityConfigs();
	public EntityConfig getEntityConfig(Class<?> clazz);
	public org.hivedb.configuration.entity.EntityConfig getEntityConfig(String clazz);
}
