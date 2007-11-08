package org.hivedb.util;

import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.IndexSchema;


public class InstallHiveIndexSchema {
	
	public static PartitionDimension install(final SingularHiveConfig hiveConfig) {
			
		try {
			// Create or update a partition dimension and its resources, secondaryIndexes and update
			// the data nodes
			new HiveSyncer(hiveConfig.getHive()).syncHive(hiveConfig);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		try {
			// Create any missing index tables
			new IndexSchema(hiveConfig.getHive().getPartitionDimension()).install();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return hiveConfig.getHive().getPartitionDimension();
	}
}
