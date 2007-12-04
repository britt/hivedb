package org.hivedb.configuration;


public class SingularHiveConfigImpl implements SingularHiveConfig {
	private final EntityConfig entityConfig;
	private Class<?> clazz;
	public SingularHiveConfigImpl(Class<?>  clazz, EntityConfig entityConfig)
	{
		this.clazz = clazz;
		this.entityConfig = entityConfig;
	}
	
	public EntityConfig getEntityConfig() {
		return entityConfig;
	}

	public String getPartitionDimensionName() {
		return entityConfig.getPartitionDimensionName();
	}

	public Class<?> getPartitionDimensionType() {
		return this.clazz;
	}
}
