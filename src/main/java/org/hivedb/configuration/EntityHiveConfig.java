package org.hivedb.configuration;

import java.util.Collection;

public interface EntityHiveConfig extends HiveConfig {
	public Collection<EntityConfig> getEntityConfigs();
	public EntityConfig getEntityConfig(Class<?> clazz);
	public EntityConfig getEntityConfig(String clazz);
}
