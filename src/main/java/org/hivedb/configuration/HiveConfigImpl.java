package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;

public class HiveConfigImpl implements SingularHiveConfig {

	private final Hive hive;
	private final EntityConfig entityConfig;
	public HiveConfigImpl(Hive hive, EntityConfig entityConfig)
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
	
	public Collection<Node> getDataNodes() {
		return hive.getPartitionDimension().getNodes();
	}
}
