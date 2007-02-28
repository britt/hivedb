///**
// * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
// * data storage systems.
// * 
// * @author Kevin Kelm (kkelm@fortress-consulting.com)
// */
//package org.hivedb.reference;
//
//public class PartitionedObjectStatus {
//
//	/**
//	 * Create status object
//	 * @param node
//	 * @param isReadOnly
//	 */
//	public PartitionedObjectStatus( String node, boolean isReadOnly, String primaryKey, boolean isPrimary ) {
//		setNodeName( node );
//		setReadOnly( isReadOnly );
//		setPrimaryKey( primaryKey );
//		setPrimary( isPrimary );
//	} // constructor
//	
//	/**
//	 * returns the node name where the object lives
//	 * @return
//	 */
//	public String getNodeName() {
//		return nodeName;
//	}
//	/**
//	 * Sets the node name
//	 * @param nodeName
//	 */
//	protected void setNodeName(String nodeName) {
//		this.nodeName = nodeName;
//	}
//	
//	/**
//	 * Returns whether or not the requested row is in read-only mode
//	 * @return
//	 */
//	public boolean isReadOnly() {
//		return isReadOnly;
//	}
//	
//	/** 
//	 * Sets readonly status
//	 * @param isReadOnly
//	 */
//	protected void setReadOnly(boolean isReadOnly) {
//		this.isReadOnly = isReadOnly;
//	}
//
//	/**
//	 * Returns the primary key pointed to by the secondary index
//	 * @return
//	 */
//	public String getPrimaryKey() {
//		return primaryKey;
//	}
//
//	/**
//	 * Sets the primary key
//	 * @param primaryKey
//	 */
//	protected void setPrimaryKey(String primaryKey) {
//		this.primaryKey = primaryKey;
//	}
//
//	/**
//	 * Returns whether this result represents a
//	 * primary or secondary index result.
//	 * @return
//	 */
//	public boolean isPrimary() {
//		return isPrimary;
//	}
//
//	/**
//	 * Sets whether this is a primary or secondary
//	 * index result.
//	 * @param isPrimary
//	 */
//	protected void setPrimary(boolean isPrimary) {
//		this.isPrimary = isPrimary;
//	}
//
//	
//	
//	protected String nodeName;
//	protected boolean isReadOnly;
//	protected String primaryKey;
//	protected boolean isPrimary;
//}
