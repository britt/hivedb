package org.hivedb.util;

import org.apache.log4j.Logger;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;


public class InstallHiveIndexSchema {
	private static Logger log = Logger.getLogger(InstallHiveIndexSchema.class);
	
	public static PartitionDimension install(final HiveScenarioConfig hiveScenarioConfig) {
			
		try {
			// Create or update a partition dimension and its resources, secondaryIndexes and update
			// the data nodes
			HiveDiff hiveDiff = new HiveSyncer(hiveScenarioConfig.getHive()).syncHive(hiveScenarioConfig);
			log.debug(hiveDiff.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		try {
			// Create any missing index tables
			for (PartitionDimension pd : hiveScenarioConfig.getHive().getPartitionDimensions())
				new IndexSchema(pd).install();
		}
		catch (Exception exception)
		{
			throw new RuntimeException(exception);
		}
		return hiveScenarioConfig.getHive().getPartitionDimension(
					hiveScenarioConfig.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName());
	}
}
