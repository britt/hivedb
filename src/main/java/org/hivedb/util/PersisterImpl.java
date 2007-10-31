package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.EntityConfig;
import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.EntityIndexConfig;
import org.hivedb.util.functional.Actor;

public class PersisterImpl implements Persister {
	public Object persistPrimaryIndexKey(HiveConfig hiveConfig, Object primaryIndexKey) {
		try {
			hiveConfig.getHive().directory().insertPrimaryIndexKey(primaryIndexKey);
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return primaryIndexKey;
	}
	
	public Object persistResourceInstance(HiveConfig hiveConfig, Object instance) {
		EntityConfig<?> entityConfig = hiveConfig.getEntityConfig();
		try {
			hiveConfig.getHive().directory().insertResourceId(
					entityConfig.getResourceName(),
					entityConfig.getId(instance),
					entityConfig.getPrimaryIndexKey(instance));
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	public Object persistSecondaryIndexKey(
			final HiveConfig hiveConfig, 
			final EntityIndexConfig entitySecondaryIndexConfig, 
			final Object resourceInstance) {
		final EntityConfig<?> entityConfig = hiveConfig.getEntityConfig();
		final SecondaryIndex secondaryIndex = getSecondaryIndex(hiveConfig.getHive(), entitySecondaryIndexConfig, entityConfig);
		
			new Actor<Object>(entitySecondaryIndexConfig.getIndexValues(resourceInstance)) {	
				public void f(Object secondaryIndexKey) {
					try {
						hiveConfig.getHive().directory().insertSecondaryIndexKey(
						    secondaryIndex.getResource().getName(),
							secondaryIndex.getName(),
							secondaryIndexKey, 
							entityConfig.getId(resourceInstance));
					} catch (HiveReadOnlyException e) {
						throw new RuntimeException(e);
					}
			}}.perform();
		return entitySecondaryIndexConfig;
	}
	
	private SecondaryIndex getSecondaryIndex(Hive hive, EntityIndexConfig secondaryIndexIdentifable, EntityConfig<?> entityConfig)
	{
		String resourceName = entityConfig.getResourceName();
		return hive.getPartitionDimension().getResource(resourceName).getSecondaryIndex(secondaryIndexIdentifable.getIndexName());
	}

	
}
