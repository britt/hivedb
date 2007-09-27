package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;

public class PersisterImpl implements Persister {
	public Object persistPrimaryIndexKey(HiveScenarioConfig hiveScenarioConfig, Object primaryIndexKey) {
		try {
			hiveScenarioConfig.getHive().insertPrimaryIndexKey(hiveScenarioConfig.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName(), primaryIndexKey);
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return primaryIndexKey;
	}
	
	public Object persistResourceInstance(HiveScenarioConfig hiveScenarioConfig, Object instance) {
		ResourceIdentifiable<Object> resourceIdentifiable = hiveScenarioConfig.getResourceIdentifiable();
		try {
			hiveScenarioConfig.getHive().insertResourceId(
					resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName(),
					resourceIdentifiable.getResourceName(),
					resourceIdentifiable.getId(instance),
					resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey(instance));
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	public Object persistSecondaryIndexKey(
			final HiveScenarioConfig hiveScenarioConfig, 
			final SecondaryIndexIdentifiable secondaryIndexIdentifiable, 
			final Object resourceInstance) {
		final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenarioConfig.getResourceIdentifiable();
		final SecondaryIndex secondaryIndex = getSecondaryIndex(hiveScenarioConfig.getHive(), secondaryIndexIdentifiable, resourceIdentifiable);
		
			new Actor<Object>(secondaryIndexIdentifiable.getSecondaryIndexValue(resourceInstance)) {	
				public void f(Object secondaryIndexKey) {
					try {
						hiveScenarioConfig.getHive().insertSecondaryIndexKey(
							secondaryIndex.getName(), 
							secondaryIndex.getResource().getName(),
							secondaryIndex.getResource().getPartitionDimension().getName(),
							secondaryIndexKey, 
							resourceIdentifiable.getId(resourceInstance));
					} catch (HiveReadOnlyException e) {
						throw new RuntimeException(e);
					}
			}}.perform();
		return secondaryIndexIdentifiable;
	}
	
	private SecondaryIndex getSecondaryIndex(Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifable, ResourceIdentifiable<Object> resourceIdentifiable)
	{
		String resourceName = resourceIdentifiable.getResourceName();
		String partitionDimensionName = resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName();
		return hive.getPartitionDimension(partitionDimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexIdentifable.getSecondaryIndexKeyPropertyName());
	}

	
}
