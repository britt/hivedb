package org.hivedb.configuration;

public interface PartitionDimensionConfig {
	String getPartitionDimensionName();
	Class<?> getPartitionDimensionType();
}