package org.hivedb.util;

import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.IndexSchema;


public class InstallHiveIndexSchema {
	
	public static PartitionDimension install(final HiveConfig hiveConfig) {
			
		try {
			// Create or update a partition dimension and its resources, secondaryIndexes and update
			// the data nodes
			new HiveSyncer(hiveConfig.getHive()).syncHive(hiveConfig);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		try {
			// Create any missing index tables
			for (PartitionDimension pd : hiveConfig.getHive().getPartitionDimensions())
				new IndexSchema(pd).install();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return hiveConfig.getHive().getPartitionDimension(
					hiveConfig.getEntityConfig().getPartitionDimensionName());
	}
}
