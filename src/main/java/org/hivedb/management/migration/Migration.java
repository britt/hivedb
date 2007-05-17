package org.hivedb.management.migration;

public interface Migration {
	public abstract String getDestination();
	public abstract String getOrigin();
	public abstract Object getPrimaryIndexKey();
	public abstract String getHiveUri();
	public abstract String getPartitionDimension();
}