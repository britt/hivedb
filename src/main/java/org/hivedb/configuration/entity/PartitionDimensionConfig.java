package org.hivedb.configuration.entity;

public interface PartitionDimensionConfig {
	String getPartitionDimensionName();
	Class<?> getPartitionDimensionType();
}