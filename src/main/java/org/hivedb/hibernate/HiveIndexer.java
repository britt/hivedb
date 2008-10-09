package org.hivedb.hibernate;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigDelegator;
import org.hivedb.util.functional.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class HiveIndexer {
  private Hive hive;

  public HiveIndexer(Hive hive) {
    this.hive = hive;
  }

  public void insert(final EntityConfig config, final Object entity) throws HiveLockableException {
    try {
      conditionallyInsertPrimaryIndexKey(config, entity);
      hive.directory().insertResourceId(config.getResourceName(), config.getId(entity), config.getPrimaryIndexKey(entity));
      conditionallyInsertDelegatedResourceIndexes(config, entity);
      insertSecondaryIndexes(config, entity);
    } catch (RuntimeException e) {
      hive.directory().deleteResourceId(config.getResourceName(), config.getId(entity));
      throw e;
    }
  }

  public void conditionallyInsertPrimaryIndexKey(final EntityConfig config,
                                                 final Object entity) throws HiveLockableException {
    if (!hive.directory().doesPrimaryIndexKeyExist(config.getPrimaryIndexKey(entity)))
      hive.directory().insertPrimaryIndexKey(config.getPrimaryIndexKey(entity));
  }

  private void conditionallyInsertDelegatedResourceIndexes(EntityConfig config, Object entity) throws HiveLockableException {
    for (EntityIndexConfig entityIndexConfig : config.getEntityIndexConfigs())
      if (entityIndexConfig.getIndexType().equals(IndexType.Delegates)) {
        final EntityIndexConfigDelegator delegateEntityConfig = ((EntityIndexConfigDelegator) entityIndexConfig);
        for (Object value : entityIndexConfig.getIndexValues(entity))
          if (!hive.directory().doesResourceIdExist(delegateEntityConfig.getDelegateEntityConfig().getResourceName(), value))
            insert(
              delegateEntityConfig.getDelegateEntityConfig(),
              delegateEntityConfig.stubEntityInstance(value, config.getPrimaryIndexKey(entity)));
      }
  }

  private void insertSecondaryIndexes(final EntityConfig config, final Object entity) throws HiveLockableException {
    Map<String, Collection<Object>> secondaryIndexMap = Transform.toMap(
      Filter.grep(
        new Filter.NotNullPredicate<Entry<String, Collection<Object>>>(),
        Transform.map(
          new Unary<EntityIndexConfig, Entry<String, Collection<Object>>>() {
            public Entry<String, Collection<Object>> f(EntityIndexConfig entityIndexConfig) {
              if (entityIndexConfig.getIndexValues(entity) == null) // Protect against null properties
                return null;
              return new Pair<String, Collection<Object>>(entityIndexConfig.getIndexName(), entityIndexConfig.getIndexValues(entity));
            }
          }, getSecondaryIndexConfigs(config))));
    hive.directory().insertSecondaryIndexKeys(config.getResourceName(), secondaryIndexMap, config.getId(entity));
  }

  private Collection<EntityIndexConfig> getSecondaryIndexConfigs(final EntityConfig config) {
    return Filter.grep(new Predicate<EntityIndexConfig>() {
      public boolean f(EntityIndexConfig entityIndexConfig) {
        return entityIndexConfig.getIndexType().equals(IndexType.Hive);
      }
    }, config.getEntityIndexConfigs());
  }

  private Collection<EntityIndexConfig> getPartitionIndexConfigs(final EntityConfig config) {
    return Filter.grep(new Predicate<EntityIndexConfig>() {
      public boolean f(EntityIndexConfig entityIndexConfig) {
        return entityIndexConfig.getIndexType().equals(IndexType.Partition);
      }
    }, config.getEntityIndexConfigs());
  }

  @SuppressWarnings("unchecked")
  private Map<String, Collection<Object>> getAllSecondaryIndexValues(EntityConfig config, Object entity) {
    Map<String, Collection<Object>> secondaryIndexMap = new HashMap<String, Collection<Object>>();
    for (EntityIndexConfig indexConfig :
      getSecondaryIndexConfigs(config))
      secondaryIndexMap.put(
        indexConfig.getIndexName(),
        hive.directory().getSecondaryIndexKeysWithResourceId(
          config.getResourceName(),
          indexConfig.getIndexName(),
          config.getId(entity)));
    return secondaryIndexMap;
  }

  private Map<String, Object> getAllPartitionIndexValues(EntityConfig config, Object entity) {
    Map<String, Object> partitionIndexMap = new HashMap<String, Object>();
    for (EntityIndexConfig indexConfig :
      getPartitionIndexConfigs(config))
      partitionIndexMap.put(
        indexConfig.getIndexName(),
        hive.directory().getPrimaryIndexKeyOfResourceId(
          config.getResourceName(),
          //indexConfig.getIndexName(),
          config.getId(entity)));
    return partitionIndexMap;
  }

  public void updatePartitionDimensionIndexIfNeeded(EntityConfig config, Object entity) throws HiveLockableException {
    //Detect partition key changes
    Object originalPartitionKey =
      hive.directory().getPrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity));
    if (!config.getPrimaryIndexKey(entity).equals(originalPartitionKey)) {
      conditionallyInsertPrimaryIndexKey(config, entity);
      hive.directory().updatePrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity), config.getPrimaryIndexKey(entity));
    }
  }

  public void update(EntityConfig config, Object entity) throws HiveLockableException {
    Map<String, Collection<Object>> secondaryIndexValues = getAllSecondaryIndexValues(config, entity);
    Map<String, Object> partitionIndexValues = getAllPartitionIndexValues(config, entity);
    Map<String, Collection<Object>> toDelete = Maps.newHashMap();
    Map<String, Collection<Object>> toInsert = Maps.newHashMap();

    conditionallyInsertDelegatedResourceIndexes(config, entity);

    for (EntityIndexConfig indexConfig : getPartitionIndexConfigs(config)) {
      if (!partitionIndexValues.get(indexConfig.getIndexName()).equals(indexConfig.getIndexValues(entity).iterator().next())) {
        if (!hive.directory().doesPrimaryIndexKeyExist(indexConfig.getIndexValues(entity).iterator().next())) {
          hive.directory().insertPrimaryIndexKey(indexConfig.getIndexValues(entity).iterator().next());
        }
        hive.directory().updatePrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity), indexConfig.getIndexValues(entity).iterator().next());
      }
    }

    // Delete and insert secondary index values that have changed
    for (EntityIndexConfig indexConfig : getSecondaryIndexConfigs(config)) {
      Pair<Collection<Object>, Collection<Object>> diff =
        Collect.diff(secondaryIndexValues.get(indexConfig.getIndexName()), indexConfig.getIndexValues(entity));
      toDelete.put(indexConfig.getIndexName(), diff.getKey());
      toInsert.put(indexConfig.getIndexName(), diff.getValue());
    }


    hive.directory().insertSecondaryIndexKeys(config.getResourceName(), toInsert, config.getId(entity));
    hive.directory().deleteSecondaryIndexKeys(config.getResourceName(), toDelete, config.getId(entity));
  }

  public void delete(EntityConfig config, Object entity) throws HiveLockableException {
    if (config.isPartitioningResource())
      hive.directory().deletePrimaryIndexKey(config.getPrimaryIndexKey(entity));
    else
      hive.directory().deleteResourceId(config.getResourceName(), config.getId(entity));
  }

  public boolean exists(EntityConfig config, Object entity) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), config.getId(entity));
  }

  public boolean idExists(EntityConfig config, Object id) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), id);
  }
}
