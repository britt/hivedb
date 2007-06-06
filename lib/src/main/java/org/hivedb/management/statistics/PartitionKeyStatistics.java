package org.hivedb.management.statistics;

import java.sql.Date;

import org.hivedb.meta.PartitionDimension;

public interface PartitionKeyStatistics {

	public abstract Object getKey();

	public abstract void setKey(Object key);

	public abstract PartitionDimension getPartitionDimension();

	public abstract void setPartitionDimension(
			PartitionDimension partitionDimension);

	public abstract int getChildRecordCount();

	public abstract void setChildRecordCount(int childRecordCount);

	public abstract Date getLastUpdated();

	public abstract void setLastUpdated(Date lastUpdated);

}