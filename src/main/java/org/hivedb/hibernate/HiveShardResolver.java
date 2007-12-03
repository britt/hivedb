package org.hivedb.hibernate;


import java.util.Collection;
import java.util.List;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.resolution.ShardResolutionStrategy;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyData;
import org.hibernate.shards.util.Lists;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveShardResolver implements ShardResolutionStrategy {
	private EntityHiveConfig hiveConfig;

	public HiveShardResolver(EntityHiveConfig hiveConfig) {
		this.hiveConfig = hiveConfig;
	}
	
	public List<ShardId> selectShardIdsFromShardResolutionStrategyData(ShardResolutionStrategyData data) {
		EntityConfig config = hiveConfig.getEntityConfig(
				resolveEntityInterface(data.getEntityName()));
		Collection<Integer> ids;
		if(config.isPartitioningResource())
			ids = hiveConfig.getHive().directory().getNodeIdsOfPrimaryIndexKey(data.getId());
		else
			ids = hiveConfig.getHive().directory().getNodeIdsOfResourceId(config.getResourceName(), data.getId());
		return Lists.newArrayList(Transform.map(nodeIdToShardIdConverter(), ids));
	}
	
	private Class<?> resolveEntityInterface(String entityName) {
		try {
			return ReflectionTools.whichIsImplemented(
					Class.forName(entityName), 
					Transform.map(new Unary<EntityConfig, Class>() {
						public Class f(EntityConfig entityConfig) {
							return entityConfig.getRepresentedInterface();
						}},
						hiveConfig.getEntityConfigs()));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public static Unary<Integer, ShardId> nodeIdToShardIdConverter() {
		return new Unary<Integer, ShardId>(){
			public ShardId f(Integer id) {
				return new ShardId(id);
			}};
	}
}
