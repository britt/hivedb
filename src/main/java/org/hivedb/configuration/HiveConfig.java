package org.hivedb.configuration;

import org.hivedb.Hive;

public interface HiveConfig {
	String getPartitionDimensionName();
	Class<?> getPartitionDimensionType();
}