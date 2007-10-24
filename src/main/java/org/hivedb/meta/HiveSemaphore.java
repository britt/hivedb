/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.Lockable;

/**
 * HiveSemaphore coordinates the Hive-global write lock, and provides a Hive revision counter
 * which indicates signals that a new partition dimension, secondary partition, or node is introduced.
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class HiveSemaphore implements Lockable{
	private boolean isReadOnly = false;
	private int revision = 0;
	
	public HiveSemaphore() {}
	
	public void setRevision(int revision) {
		this.revision = revision;
	}

	public HiveSemaphore(boolean isReadOnly, int revision) {
		this.isReadOnly = isReadOnly;
		this.revision = revision;		
	}
	public boolean isReadOnly() {
		return isReadOnly;
	}
	public void setReadOnly(boolean isReadOnly) {
		this.isReadOnly = isReadOnly;
	}
	public int getRevision() {
		return revision;
	}
	public void incrementRevision() {
		revision++;
	}	
}
