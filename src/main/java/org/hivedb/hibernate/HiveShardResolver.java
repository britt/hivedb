package org.hivedb.hibernate;


import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.resolution.ShardResolutionStrategy;
import org.hibernate.shards.strategy.selection.ShardResolutionStrategyData;
import org.hibernate.shards.util.Lists;
import org.hivedb.Hive;
import org.hivedb.annotations.HiveForeignKey;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveShardResolver implements ShardResolutionStrategy {
	private EntityHiveConfig hiveConfig;
	private Hive hive;

	public HiveShardResolver(EntityHiveConfig hiveConfig, Hive hive) {
		this.hiveConfig = hiveConfig;
		this.hive = hive;
	}
	
	public List<ShardId> selectShardIdsFromShardResolutionStrategyData(ShardResolutionStrategyData data) {
		final Class clazz;
		try {
			clazz = Class.forName(data.getEntityName());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		final Class<?> resolvedEntityInterface = new EntityResolver(hiveConfig).resolveToEntityOrRelatedEntity(clazz);
		EntityConfig config = hiveConfig.getEntityConfig(
				resolvedEntityInterface);
		Collection<Integer> ids = (config.isPartitioningResource())
			? hive.directory().getNodeIdsOfPrimaryIndexKey(data.getId())
			: hive.directory().getNodeIdsOfResourceId(config.getResourceName(), data.getId());
		return Lists.newArrayList(Transform.map(nodeIdToShardIdConverter(), ids));
	}
	
	

	public static Unary<Integer, ShardId> nodeIdToShardIdConverter() {
		return new Unary<Integer, ShardId>(){
			public ShardId f(Integer id) {
				return new ShardId(id);
			}};
	}
}
