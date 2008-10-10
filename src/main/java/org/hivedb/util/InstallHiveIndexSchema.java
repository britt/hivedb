package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.database.Schemas;

public class InstallHiveIndexSchema {

  public static PartitionDimension install(final EntityHiveConfig entityHiveConfig, Hive hive) {

    try {
      // Create or update a partition dimension and its resources, secondaryIndexes and update
      // the data nodes
      new HiveSyncer(hive).syncHive(entityHiveConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      // Create any missing index tables
      Schemas.install(hive.getPartitionDimension());
    }
    catch (Exception exception) {
      throw new RuntimeException(exception);
    }
    return hive.getPartitionDimension();
  }
}
