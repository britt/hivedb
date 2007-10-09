package org.hivedb.util;

import org.apache.log4j.Logger;
import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.PartitionDimension;


public class InstallHiveIndexSchema {
	private static Logger log = Logger.getLogger(InstallHiveIndexSchema.class);
	
	public static PartitionDimension install(final HiveConfig hiveConfig) {
			
		try {
			// Create or update a partition dimension and its resources, secondaryIndexes and update
			// the data nodes
			HiveDiff hiveDiff = new HiveSyncer(hiveConfig.getHive()).syncHive(hiveConfig);
			log.debug(hiveDiff.toString());
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
