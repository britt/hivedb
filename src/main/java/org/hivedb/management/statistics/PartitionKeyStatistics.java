package org.hivedb.management.statistics;

import java.sql.Date;

import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.HiveUtils;

// TODO Naming incongruity, all of the other *Statistics are interfaces
public class PartitionKeyStatistics implements Comparable, Cloneable{
	private PartitionDimension partitionDimension;
	private Object key;
	private int childRecordCount = 0;
	private Date lastUpdated;
	
	public PartitionKeyStatistics(PartitionDimension partitionDimension, Object key, Date lastUpdated){
		setLastUpdated(lastUpdated);
		setPartitionDimension(partitionDimension);
		setKey(key);
	}
	
	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}

	public void setPartitionDimension(PartitionDimension partitionDimension) {
		this.partitionDimension = partitionDimension;
	}
	
	public int hashCode() {
		return HiveUtils.makeHashCode(
			new Object[] {getKey(), getChildRecordCount(), getPartitionDimension(), getLastUpdated()}
		);
	} 
	
	public boolean equals(Object o) {
		return this.hashCode() == o.hashCode();
	}

	public int getChildRecordCount() {
		return childRecordCount;
	}

	public void setChildRecordCount(int childRecordCount) {
		this.childRecordCount = childRecordCount;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public int compareTo(Object arg0) {
		PartitionKeyStatistics s = (PartitionKeyStatistics)arg0;
		if(this.equals(s))
			return 0;
		else if(this.getChildRecordCount() > s.getChildRecordCount())
			return 1;
		else if(this.getChildRecordCount() < s.getChildRecordCount())
			return -1;
		else
			return new Integer(this.hashCode()).compareTo(s.hashCode());
	}
	
	public PartitionKeyStatistics clone() {
		PartitionKeyStatistics clone = new PartitionKeyStatistics(getPartitionDimension(), getKey(), (Date) getLastUpdated().clone());
		clone.setChildRecordCount(getChildRecordCount());
		return clone;
	}
}
