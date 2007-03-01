package org.hivedb.management.statistics;

/**
 * 
 * 
 * @author Justin McCarthy
 */
public interface NodeFillStatistics {
	public long getCapacity();
	public void setCapacity(long capacity);
	public long getFill();
	public void setFill(long fill);
	public float getPercentage();
}
