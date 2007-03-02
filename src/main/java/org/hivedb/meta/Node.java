/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.util.HiveUtils;

/**
 * Node models a database instance suitable for storage partitioned Data.
 * 
 * Node does not currently model a database suitable for Index storage.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Node implements Comparable<Node>, Cloneable, Identifiable {
	private int id;
	private String uri;
	private NodeGroup nodeGroup;
	private boolean readOnly;
	private double capacity;

	public Node(String uri) {
		this(uri, false);
	}
	
	public Node(String uri, boolean readOnly) {
		this(Hive.NEW_OBJECT_ID, uri, readOnly);
	}

	/**
	 * 
	 * @param id
	 * @param uri
	 * @param access
	 * @param readOnly
	 */
	public Node(int id, String uri, boolean readOnly) {
		super();
		this.id = id;
		this.uri = uri;
		this.readOnly = readOnly;
	}

	public int getId() {
		return id;
	}
	public boolean isReadOnly() {
		return readOnly;
	}
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public NodeGroup getNodeGroup() {
		return nodeGroup;
	}
	public double getCapacity() {
		return capacity;
	}
	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}
	
	/**
	 *  INTERNAL USE ONLY -- Sets the nodegroup to which this node belongs
	 * @param nodeGroup
	 */
	public void setNodeGroup(NodeGroup nodeGroup) {
		this.nodeGroup = nodeGroup;
	}
	

	/**
	 * For use by persistence layer and unit tests.  Otherwise, id should be considered immmutable.
	 * 
	 * @param id Database-generated identifier with which this instance should be updated
	 */
	public void updateId(int id) {
		this.id = id;
	}	

	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				uri, readOnly
		});
	}
	
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
										"Id", 		getId(), 
										"Uri", 		getUri(), 
										"ReadOnly",	isReadOnly());									
	}

	public int compareTo(Node o) {
		return getUri().compareTo(o.getUri());
	}
	
	/**
	 * Clones the immediate fields of this object, for use in testing
	 */
	public Object clone()
	{
		return new Node(uri,readOnly);
	}
}
