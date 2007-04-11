package org.hivedb.management.statistics;

import java.sql.Date;

import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.HiveUtils;

public class PartitionKeyStatisticsBean implements Comparable, Cloneable, PartitionKeyStatistics{
	private PartitionDimension partitionDimension;
	private Object key;
	private int childRecordCount = 0;
	private Date lastUpdated;
	
	public PartitionKeyStatisticsBean(PartitionDimension partitionDimension, Object key, Date lastUpdated){
		setLastUpdated(lastUpdated);
		setPartitionDimension(partitionDimension);
		setKey(key);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#getKey()
	 */
	public Object getKey() {
		return key;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#setKey(java.lang.Object)
	 */
	public void setKey(Object key) {
		this.key = key;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#getPartitionDimension()
	 */
	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#setPartitionDimension(org.hivedb.meta.PartitionDimension)
	 */
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

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#getChildRecordCount()
	 */
	public int getChildRecordCount() {
		return childRecordCount;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#setChildRecordCount(int)
	 */
	public void setChildRecordCount(int childRecordCount) {
		this.childRecordCount = childRecordCount;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#getLastUpdated()
	 */
	public Date getLastUpdated() {
		return lastUpdated;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.PartitionKeyStatistics#setLastUpdated(java.sql.Date)
	 */
	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public int compareTo(Object arg0) {
		PartitionKeyStatisticsBean s = (PartitionKeyStatisticsBean)arg0;
		if(this.equals(s))
			return 0;
		else if(this.getChildRecordCount() > s.getChildRecordCount())
			return 1;
		else if(this.getChildRecordCount() < s.getChildRecordCount())
			return -1;
		else
			return new Integer(this.hashCode()).compareTo(s.hashCode());
	}
	
	public PartitionKeyStatisticsBean clone() {
		PartitionKeyStatisticsBean clone = new PartitionKeyStatisticsBean(getPartitionDimension(), getKey(), (Date) getLastUpdated().clone());
		clone.setChildRecordCount(getChildRecordCount());
		return clone;
	}
}
