package org.hivedb.hibernate.simplified.configuration;

import org.hivedb.configuration.EntityConfig;

import java.util.Collection;

public interface HiveConfig {
	public String getPartitionDimensionName();
	public Class<?> getPartitionDimensionType();
	public Collection<EntityConfig> getEntityConfigs();
	public EntityConfig getEntityConfig(Class<?> clazz);
	public EntityConfig getEntityConfig(String clazz);
}
