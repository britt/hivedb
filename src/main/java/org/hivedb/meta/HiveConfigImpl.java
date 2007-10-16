package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.Hive;

public class HiveConfigImpl implements HiveConfig {

	private final Hive hive;
	private final EntityConfig<?> entityConfig;
	public HiveConfigImpl(Hive hive, EntityConfig<?> entityConfig)
	{
		this.hive = hive;
		this.entityConfig = entityConfig;
	}
	
	public EntityConfig<?> getEntityConfig() {
		return entityConfig;
	}

	public Hive getHive() {
		return hive;
	}
	
	public Collection<Node> getDataNodes() {
		return hive.getPartitionDimension(entityConfig.getPartitionDimensionName()).getNodes();
	}
}
