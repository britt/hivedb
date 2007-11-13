package org.hivedb.configuration;

import org.hivedb.Hive;

public interface HiveConfig {
	Hive getHive();
	String getPartitionDimensionName();
	Class<?> getPartitionDimensionType();
}