package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.functional.Actor;

public class PersisterImpl implements Persister {
	private Hive hive;
	
	public PersisterImpl(Hive hive) {
		this.hive = hive;
	}
	
	@SuppressWarnings("unchecked")
	public Object persistPrimaryIndexKey(EntityHiveConfig entityHiveConfig, Class representedInterface, Object primaryIndexKey) {
		try {
			hive.directory().insertPrimaryIndexKey(primaryIndexKey);
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return primaryIndexKey;
	}
	
	@SuppressWarnings("unchecked")
	public Object persistResourceInstance(EntityHiveConfig entityHiveConfig, Class representedInterface, Object instance) {
		EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
		try {
			hive.directory().insertResourceId(
					entityConfig.getResourceName(),
					entityConfig.getId(instance),
					entityConfig.getPrimaryIndexKey(instance));
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	public Object persistSecondaryIndexKey(
			final EntityHiveConfig entityHiveConfig,
			final Class representedInterface,
			final EntityIndexConfig entitySecondaryIndexConfig, 
			final Object resourceInstance) {
		final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
		final SecondaryIndex secondaryIndex = getSecondaryIndex(hive, entitySecondaryIndexConfig, entityConfig);
		
			new Actor<Object>(entitySecondaryIndexConfig.getIndexValues(resourceInstance)) {	
				public void f(Object secondaryIndexKey) {
					try {
						hive.directory().insertSecondaryIndexKey(
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
	
	private SecondaryIndex getSecondaryIndex(HiveFacade hive, EntityIndexConfig secondaryIndexIdentifable, EntityConfig entityConfig)
	{
		String resourceName = entityConfig.getResourceName();
		return hive.getPartitionDimension().getResource(resourceName).getSecondaryIndex(secondaryIndexIdentifable.getIndexName());
	}

	
}
