package org.hivedb.util;

import java.sql.SQLException;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.scenarioBuilder.PrimaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.ResourceIdentifiable;
import org.hivedb.util.scenarioBuilder.SecondaryIndexIdentifiable;

public class PersisterImpl implements Persister {
	public PrimaryIndexIdentifiable persistPrimaryIndexIdentifiable(final Hive hive, final PrimaryIndexIdentifiable primaryIndexIdentifiable) throws HiveException, SQLException {
		hive.insertPrimaryIndexKey(hive.getPartitionDimension(primaryIndexIdentifiable.getPartitionDimensionName()), primaryIndexIdentifiable.getPrimaryIndexKey());
		return primaryIndexIdentifiable;
	}
	
	public ResourceIdentifiable persistResourceIdentifiableInstance(Hive hive, ResourceIdentifiable resourceIdentifiable) {
		// The hive doesn't care about individual instances representing resources, so nothing needs to be done here.
		// You could override this to persist data to you data node for this ResourceIdentifiable instance.
		// The return value allows you to return an instance that has been given an id after saving
		return resourceIdentifiable;
	}
	
	public SecondaryIndexIdentifiable persistSecondaryIndexIdentifiableInstance(final Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifiable) throws HiveException, SQLException {
		hive.insertSecondaryIndexKey(getSecondaryIndex(hive, secondaryIndexIdentifiable), secondaryIndexIdentifiable.getSecondaryIndexKey(), getPrimaryIndexKey(secondaryIndexIdentifiable));
		return secondaryIndexIdentifiable;
	}
	
	private SecondaryIndex getSecondaryIndex(Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifable)
	{
		ResourceIdentifiable resourceIdentifiable = secondaryIndexIdentifable.getResourceIdentifiable();
		String resourceName = resourceIdentifiable.getResourceName();
		String partitionDimensionName = resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName();
		try {
			return hive.getPartitionDimension(partitionDimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexIdentifable.getSecondaryIndexName());
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
	private Object getPrimaryIndexKey(SecondaryIndexIdentifiable secondaryIndexIdentifiable)
	{
		return secondaryIndexIdentifiable.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPrimaryIndexKey();
	}
}
