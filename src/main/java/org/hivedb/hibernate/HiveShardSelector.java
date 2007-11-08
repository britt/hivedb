package org.hivedb.hibernate;

import java.util.Collection;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardSelectionStrategy;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;

public class HiveShardSelector implements ShardSelectionStrategy {
	private EntityHiveConfig hiveConfig;
	
	public HiveShardSelector(EntityHiveConfig hiveConfig) {
		this.hiveConfig = hiveConfig;
	}
	
	// The Hive HAS to be responsible for shard allocation
	public ShardId selectShardIdForNewObject(Object entity) {
		EntityConfig config = hiveConfig.getEntityConfig(entity.getClass());
		
		if(!hiveConfig.getHive().directory().doesPrimaryIndexKeyExist(config.getPrimaryIndexKey(entity)))
			try {
				hiveConfig.getHive().directory().insertPrimaryIndexKey(config.getPrimaryIndexKey(entity));
			} catch (HiveReadOnlyException e) {
				throw new HiveRuntimeException(e.getMessage(), e);
			}
		
		Collection<Integer> nodeIds = 
			hiveConfig.getHive().directory().getNodeIdsOfPrimaryIndexKey(config.getPrimaryIndexKey(entity));
		
		return Atom.getFirstOrThrow(Transform.map(HiveShardResolver.nodeIdToShardIdConverter(), nodeIds));
	}
}
