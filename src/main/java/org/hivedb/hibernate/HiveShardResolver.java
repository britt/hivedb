package org.hivedb.hibernate;


import java.util.Collection;
import java.util.List;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.resolution.ShardResolutionStrategy;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyData;
import org.hibernate.shards.util.Lists;
import org.hivedb.HiveFacade;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveShardResolver implements ShardResolutionStrategy {
	private EntityHiveConfig hiveConfig;
	private HiveFacade hive;

	public HiveShardResolver(EntityHiveConfig hiveConfig, HiveFacade hive) {
		this.hiveConfig = hiveConfig;
		this.hive = hive;
	}
	
	@SuppressWarnings("unchecked")
	public List<ShardId> selectShardIdsFromShardResolutionStrategyData(ShardResolutionStrategyData data) {
		
		final Class clazz;
		try {
			clazz = Class.forName(data.getEntityName());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		final Class<?> resolvedEntityInterface = new EntityResolver(hiveConfig).resolveToEntityOrRelatedEntity(clazz);
		if (resolvedEntityInterface != null) {
			EntityConfig config = hiveConfig.getEntityConfig(resolvedEntityInterface);
			Collection<Integer> ids = (config.isPartitioningResource())
				? hive.directory().getNodeIdsOfPrimaryIndexKey(data.getId())
				: hive.directory().getNodeIdsOfResourceId(config.getResourceName(), data.getId());
			return Lists.newArrayList(Transform.map(nodeIdToShardIdConverter(), ids));
		}
		else {		
			// Only for Hibernate entities that are not Hive entities, but are used for secondary indexes
			for (EntityConfig entityConfig : hiveConfig.getEntityConfigs()) {
				for (EntityIndexConfig entityIndexConfig : entityConfig.getEntityIndexConfigs()) {
					if (ReflectionTools.isComplexCollectionItemProperty(entityConfig.getRepresentedInterface(), entityIndexConfig.getPropertyName()))
						if (ReflectionTools.doesImplementOrExtend(clazz, ReflectionTools.getCollectionItemType(entityConfig.getRepresentedInterface(), entityIndexConfig.getPropertyName()))) {
							Collection<Integer> ids = hive.directory().getNodeIdsOfSecondaryIndexKey(
									entityConfig.getResourceName(),
									hive.getPartitionDimension().getResource(entityConfig.getResourceName()).getSecondaryIndex(entityIndexConfig.getIndexName()).getName(),
									data.getId());
							return Lists.newArrayList(Transform.map(nodeIdToShardIdConverter(), ids));
						}
				}
			}
		}
		throw new RuntimeException(String.format("Could not resolve class to a Hive entity nor a secondary index Hibernate entity: %s", clazz.getSimpleName()));
	}
	
	public static Unary<Integer, ShardId> nodeIdToShardIdConverter() {
		return new Unary<Integer, ShardId>(){
			public ShardId f(Integer id) {
				return new ShardId(id);
			}};
	}
}
