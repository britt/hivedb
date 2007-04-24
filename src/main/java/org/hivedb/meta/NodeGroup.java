/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.util.HiveUtils;

/**
 * NodeGroup is a container of Nodes that serve identical schema (but not necessarily
 * identical data).  NodeGroup also serves as the nexus for handing out Connections
 * between data-identical collections of Nodes (sets of servers that use native DB
 * replication to scale reads).
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class NodeGroup implements Finder {
	private int id;
	private Collection<Node> nodes;
	private PartitionDimension partitionDimension;
	
	/**
	 * Create constructor
	 * 
	 * @param partitionDimension
	 * @param access
	 */
	public NodeGroup(Collection<Node> nodes) {
		this(Hive.NEW_OBJECT_ID, nodes);
	}
	
	/**
	 * 
	 * PERSISTENCE LOAD ONLY-- load a NodeGroup from persistence. 
	 * The reference to PartitionDimension is set by PartitionDimension constructor.
	 * 
	 * @param id
	 * @param nodes
	 */
	public NodeGroup(int id, Collection<Node> nodes) {
		super();
		this.id = id;
		this.nodes = insetThisInstance(nodes);
	}
	private Collection<Node> insetThisInstance(Collection<Node> nodes)
	{
		for (Node node : nodes)
			node.setNodeGroup(this);
		return nodes;
	}
	public void add(Node node) {
		node.setNodeGroup(this);
	}
	public int getId() {
		return id;
	}
	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}
	public void setPartitionDimension(PartitionDimension partitionDimension) {
		this.partitionDimension = partitionDimension;
	}
	public Collection<Node> getNodes() {
		return nodes;
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		return (Collection<T>) getNodes();
	}
	
	public void setNodes(Collection<Node> nodes) {
		this.nodes = nodes;
	}
	public Node getNode(int nodeId) throws HiveException 
	{
		for (Node node : getNodes())
			if (node.getId() == (nodeId))
				return node;
		throw new HiveException("Node with id " + nodeId + " is not in this node group");
	}
	public Node getNode(String nodeUri) throws HiveException 
	{
		for (Node node : getNodes())
			if (node.getUri().equals(nodeUri))
				return node;
		throw new HiveException("Node with uri " + nodeUri + " is not in this node group");
	}

	public <T extends Nameable> T findByName(Class<T> forClass, String name) throws HiveException {
		return findByName(forClass, name);
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
			nodes
		});
	}
	
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
											"Id", 	 getId(), 
											"Nodes", getNodes());
	}
}
