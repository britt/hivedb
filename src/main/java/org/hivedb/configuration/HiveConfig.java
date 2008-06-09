package org.hivedb.configuration;

public interface HiveConfig {
	String getPartitionDimensionName();
	Class<?> getPartitionDimensionType();
}