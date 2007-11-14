package org.hivedb.hibernate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.util.functional.Collect;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveIndexer {
	private Hive hive;
	
	public HiveIndexer(Hive hive) {
		this.hive = hive;
	}
	
	public void insert(final EntityConfig config, final Object entity) throws HiveReadOnlyException{
		try {
			conditionallyInsertPrimaryIndexKey(config, entity);
			hive.directory().insertResourceId(config.getResourceName(), config.getId(entity), config.getPrimaryIndexKey(entity));
			insertSecondaryIndexes(config, entity);
		} catch(RuntimeException e) {
			hive.directory().deleteResourceId(config.getResourceName(), config.getId(entity));
			throw e;
		}
	}

	private void conditionallyInsertPrimaryIndexKey(final EntityConfig config,
			final Object entity) throws HiveReadOnlyException {
		if(!hive.directory().doesPrimaryIndexKeyExist(config.getPrimaryIndexKey(entity)))
			hive.directory().insertPrimaryIndexKey(config.getPrimaryIndexKey(entity));
	}
	
	@SuppressWarnings("unchecked")
	private void insertSecondaryIndexes(final EntityConfig config, final Object entity) throws HiveReadOnlyException {
		Map<String, Collection<Object>> secondaryIndexMap = Transform.toMap(
			Transform.map(
				new Unary<EntityIndexConfig, Entry<String, Collection<Object>>>(){
					public Entry<String, Collection<Object>> f(EntityIndexConfig item) {
						return new Pair<String, Collection<Object>>(item.getIndexName(), item.getIndexValues(entity));
					}
				}, config.getEntitySecondaryIndexConfigs())
		);
		hive.directory().insertSecondaryIndexKeys(config.getResourceName(), secondaryIndexMap, config.getId(entity));
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Collection<Object>> getAllSecondaryIndexValues(EntityConfig config, Object entity) {
		Map<String, Collection<Object>> secondaryIndexMap = new HashMap<String, Collection<Object>>();
		for(EntityIndexConfig indexConfig : (Collection<EntityIndexConfig>)config.getEntitySecondaryIndexConfigs())
			secondaryIndexMap.put(
					indexConfig.getIndexName(), 
					hive.directory().getSecondaryIndexKeysWithResourceId(
							config.getResourceName(), 
							indexConfig.getIndexName(), 
							config.getId(entity)));
		return secondaryIndexMap;
	}
	
	@SuppressWarnings("unchecked")
	public void update(EntityConfig config, Object entity) throws HiveReadOnlyException {
		Map<String, Collection<Object>> secondaryIndexValues = getAllSecondaryIndexValues(config, entity);
		Map<String, Collection<Object>> toDelete = Maps.newHashMap();
		Map<String, Collection<Object>> toInsert = Maps.newHashMap();
		
		for(EntityIndexConfig indexConfig : (Collection<EntityIndexConfig>)config.getEntitySecondaryIndexConfigs()) {
			Pair<Collection<Object>, Collection<Object>> diff = 
				Collect.diff(secondaryIndexValues.get(indexConfig.getIndexName()), indexConfig.getIndexValues(entity));
			toDelete.put(indexConfig.getIndexName(), diff.getKey());
			toInsert.put(indexConfig.getIndexName(), diff.getValue());
		}
		
		//Detect partition key changes
		Object originalPartitionKey = 
			hive.directory().getPrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity));
		if(!config.getPrimaryIndexKey(entity).equals(originalPartitionKey)) {
			conditionallyInsertPrimaryIndexKey(config, entity);
			hive.directory().updatePrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity), config.getPrimaryIndexKey(entity));
		}
		hive.directory().insertSecondaryIndexKeys(config.getResourceName(), toInsert, config.getId(entity));
		hive.directory().deleteSecondaryIndexKeys(config.getResourceName(), toDelete, config.getId(entity));
	}
	
	public void delete(EntityConfig config, Object entity) throws HiveReadOnlyException {
		if(config.isPartitioningResource())
			hive.directory().deletePrimaryIndexKey(config.getPrimaryIndexKey(entity));
		else
			hive.directory().deleteResourceId(config.getResourceName(), config.getId(entity));
	}
	
	public boolean exists(EntityConfig config, Object entity) {
		return hive.directory().doesResourceIdExist(config.getResourceName(), config.getId(entity));
	}
}
