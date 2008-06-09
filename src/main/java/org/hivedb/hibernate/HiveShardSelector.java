package org.hivedb.hibernate;

import java.util.Collection;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardSelectionStrategy;
import org.hivedb.HiveFacade;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveShardSelector implements ShardSelectionStrategy {
	private EntityHiveConfig hiveConfig;
	private HiveFacade hive;
	public HiveShardSelector(EntityHiveConfig hiveConfig, HiveFacade hive) {
		this.hiveConfig = hiveConfig;
		this.hive = hive;
	}
	
	// The Hive HAS to be responsible for shard allocation
	public ShardId selectShardIdForNewObject(Object entity) {
		EntityConfig config = hiveConfig.getEntityConfig(resolveEntityConfigClass(entity.getClass()));
		
		if(!hive.directory().doesPrimaryIndexKeyExist(config.getPrimaryIndexKey(entity)))
			try {
				hive.directory().insertPrimaryIndexKey(config.getPrimaryIndexKey(entity));
			} catch (HiveLockableException e) {
				throw new HiveRuntimeException(e.getMessage(), e);
			}
		
		Collection<Integer> nodeIds = 
			hive.directory().getNodeIdsOfPrimaryIndexKey(config.getPrimaryIndexKey(entity));
		
		return Atom.getFirstOrThrow(Transform.map(HiveShardResolver.nodeIdToShardIdConverter(), nodeIds));
	}
	
	@SuppressWarnings("unchecked")
	private Class<?> resolveEntityConfigClass(Class<?> clazz) {
		return ReflectionTools.whichIsImplemented(
				clazz,
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					hiveConfig.getEntityConfigs()));
	}
}
