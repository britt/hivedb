package org.hivedb.management;

/**
 * 
 * 
 * @author Justin McCarthy
 */
public interface FillStatisticsMBean {
	public long getCapacity();
	public void setCapacity(long capacity);
	public long getFill();
	public void setFill(long fill);
	public float getPercentage();
}
