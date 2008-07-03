package org.hivedb;

import java.util.Collection;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.Node;
import org.hivedb.util.Lists;

/***
 * A node assigner that uses a hash function to allocate partition keys
 * to a large number of virtual buckets.  One node may contain many buckets,
 * allowing you to easily subdivide a node at a later time. This is very 
 * similar to the concept of virtual shards in Hibernate Shards.
 * Here it is being used as a simple solution for directory partitioning. 
 * @author bcrawford
 *
 */
public class BucketAssigner implements Assigner {
	private int bucketCount = 1024;
	
	public BucketAssigner() {}
	
	public BucketAssigner(int bucketCount) {
		this.bucketCount = bucketCount;
	}
	
	public Node chooseNode(Collection<Node> nodes, Object value) {
		return getNode(nodes, getBucket(value));
	}

	public Collection<Node> chooseNodes(Collection<Node> nodes, Object value) {
		return Lists.newList(new Node[]{chooseNode(nodes, value)});
	}
	
	private Node getNode(Collection<Node> nodes, int bucket) {
		return Lists.newList(nodes).get(bucket % nodes.size());
	}
	
	private int getBucket(Object value) {
		return castAsNumber(value).intValue() % getBucketCount();
	}
	
	private Number castAsNumber(Object value) {
		if(value.getClass() == Integer.class ||  value.getClass() == int.class)
			return (Integer)value;
		else if(value.getClass() == Long.class ||  value.getClass() == long.class)
			return (Long)value;
		else
			throw new UnsupportedOperationException(String.format("Cannot convert object of type %s, the object must be a number.", value.getClass()));
	}

	public int getBucketCount() {
		return bucketCount;
	}

	public void setBucketCount(int bucketCount) {
		this.bucketCount = bucketCount;
	}
	
}
