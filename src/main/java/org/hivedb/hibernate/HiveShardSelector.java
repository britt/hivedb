package org.hivedb.hibernate;

import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardSelectionStrategy;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.entity.EntityConfig;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.util.Preconditions;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;

import java.util.Collection;

public class HiveShardSelector implements ShardSelectionStrategy {
  private EntityHiveConfig entityHiveConfig;
  private DirectoryFacade directory;

  public HiveShardSelector(EntityHiveConfig hiveConfig, DirectoryFacade directory) {
    this.entityHiveConfig = hiveConfig;
    this.directory = directory;
  }

  // The Hive MUST to be responsible for shard allocation
  public ShardId selectShardIdForNewObject(Object entity) {
    EntityConfig config = entityHiveConfig.getEntityConfig(entity.getClass());

    Preconditions.isNotNull(config);

    Object key = config.getPartitionKey(entity);
    if (!directory.doesPartitionKeyExist(key))
      try {
        directory.insertPartitionKey(key);
      } catch (HiveLockableException e) {
        throw new HiveRuntimeException(e.getMessage(), e);
      }

    Collection<Integer> nodeIds = directory.getNodeIdsOfPartitionKey(key);

    return Atom.getFirstOrThrow(Transform.map(HiveShardResolver.nodeIdToShardIdConverter(), nodeIds));
  }
}
