/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.Hive;
import org.hivedb.Lockable;
import org.hivedb.util.HiveUtils;

/**
 * Node models a database instance suitable for storage of partitioned Data.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Node implements Comparable<Node>, Cloneable, IdAndNameIdentifiable, Lockable {
	private int id,partitionDimensionId;
	private String uri,name;
	private PartitionDimension partitionDimension;
	private boolean readOnly;
	private double capacity;

	public Node(String name, String uri) {
		this(name, uri, false);
	}
	
	public Node(String name, String uri, boolean readOnly) {
		this(Hive.NEW_OBJECT_ID, name, uri, readOnly,0);
	}

	/**
	 * 
	 * @param id
	 * @param uri
	 * @param access
	 * @param readOnly
	 */
	public Node(int id, String name, String uri, boolean readOnly, int partitionDimensionId) {
		super();
		this.id = id;
		this.uri = uri;
		this.name = name;
		this.readOnly = readOnly;
		this.partitionDimensionId = partitionDimensionId;
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
	
	public double getCapacity() {
		return capacity;
	}
	public void setCapacity(double capacity) {
		this.capacity = capacity;
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
										"Name", 	getName(), 
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
		return new Node(name,uri,readOnly);
	}

	public String getName() {
		return name;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}

	public void setPartitionDimension(PartitionDimension partitionDimension) {
		this.partitionDimension = partitionDimension;
	}

	public int getPartitionDimensionId() {
		return partitionDimensionId;
	}

	public void setPartitionDimensionId(int partitionDimensionId) {
		this.partitionDimensionId = partitionDimensionId;
	}
}
