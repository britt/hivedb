package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;

public class PersisterImpl implements Persister {
	public PrimaryIndexIdentifiable persistPrimaryIndexIdentifiable(final Hive hive, final PrimaryIndexIdentifiable primaryIndexIdentifiable) {
		try {
			hive.insertPrimaryIndexKey(primaryIndexIdentifiable.getPartitionDimensionName(), primaryIndexIdentifiable.getPrimaryIndexKey());
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return primaryIndexIdentifiable;
	}
	
	public ResourceIdentifiable persistResourceIdentifiableInstance(Hive hive, ResourceIdentifiable resourceIdentifiable) {
		try {
			hive.insertResourceId(
					resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName(),
					resourceIdentifiable.getResourceName(),
					resourceIdentifiable.getId(),
					resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey());
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return resourceIdentifiable;
	}
	
	public SecondaryIndexIdentifiable persistSecondaryIndexIdentifiableInstance(final Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifiable) {
		SecondaryIndex secondaryIndex = getSecondaryIndex(hive, secondaryIndexIdentifiable);
		try {
			hive.insertSecondaryIndexKey(
					secondaryIndex.getName(), 
					secondaryIndex.getResource().getName(),
					secondaryIndex.getResource().getPartitionDimension().getName(),
					secondaryIndexIdentifiable.getSecondaryIndexKey(), 
					secondaryIndexIdentifiable.getResourceIdentifiable().getId());
		} catch (HiveReadOnlyException e) {
			throw new RuntimeException(e);
		}
		return secondaryIndexIdentifiable;
	}
	
	private SecondaryIndex getSecondaryIndex(Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifable)
	{
		ResourceIdentifiable resourceIdentifiable = secondaryIndexIdentifable.getResourceIdentifiable();
		String resourceName = resourceIdentifiable.getResourceName();
		String partitionDimensionName = resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName();
		return hive.getPartitionDimension(partitionDimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexIdentifable.getSecondaryIndexName());
	}
}
