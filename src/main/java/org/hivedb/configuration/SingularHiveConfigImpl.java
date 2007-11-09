package org.hivedb.configuration;

import org.hivedb.Hive;

public class SingularHiveConfigImpl implements SingularHiveConfig {

	private final Hive hive;
	private final EntityConfig entityConfig;
	public SingularHiveConfigImpl(Hive hive, EntityConfig entityConfig)
	{
		this.hive = hive;
		this.entityConfig = entityConfig;
	}
	
	public EntityConfig getEntityConfig() {
		return entityConfig;
	}

	public Hive getHive() {
		return hive;
	}
}
