package org.hivedb.configuration;

import org.hivedb.Hive;
import org.hivedb.util.database.JdbcTypeMapper;

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

	public String getPartitionDimensionName() {
		return entityConfig.getPartitionDimensionName();
	}

	public Class<?> getPartitionDimensionType() {
		return JdbcTypeMapper.jdbcTypeToPrimitiveClass(hive.getPartitionDimension().getColumnType());
	}
}
