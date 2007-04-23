/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.JdbcTypeMapper;

/**
 * PartitionDimension is the value we use to distribute records to data nodes.  It is
 * the central node of a related graph of secondary indexes, groups, and nodes.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class PartitionDimension implements Comparable<PartitionDimension>, Cloneable, IdAndNameIdentifiable, Finder {
	private int id;
	private String name;
	private int columnType;
	private NodeGroup nodeGroup;
	private String indexUri;
	private Collection<Resource> resources;

	private Assigner assigner = new Assigner() {
		private Random random = new Random(new Date().getTime());	
		public Node chooseNode(Collection<Node> nodes, Object value) {
			if (nodes.size()==0) throw new HiveRuntimeException("The Hive has no Nodes; the Assigner cannot make a choice.");
			return new ArrayList<Node>(nodes).get(random.nextInt(nodes.size()));
		}		
	};

	/**
	 * Create constructor
	 * 
	 * @param name
	 * @param columnType
	 * @param nodeGroup
	 * @param indexUri The URI for the PartitionDimension's index tables.
	 *  Specify this if it is different then that of the hive.
	 * @param resources
	 * @param assigner
	 */
	public PartitionDimension(String name, int columnType, NodeGroup nodeGroup,
			String indexUri, Collection<Resource> resources) {
		this(Hive.NEW_OBJECT_ID, name, columnType, nodeGroup, indexUri,
				resources);
	}
	/**
	 * 
	 * Create constructor. This version does not require an index URI. The index
	 * URI will be inherited from the hive so that the indexes are stored at the same
	 * URI as the hive metadata. This should be the typical configuration.
	 * 
	 * @param name
	 * @param columnType
	 * @param nodeGroup
	 * @param resources
	 */
	public PartitionDimension(String name, int columnType, NodeGroup nodeGroup,
			Collection<Resource> resources) {
		this(Hive.NEW_OBJECT_ID, name, columnType, nodeGroup, null,
				resources);
	}
	
	/**
	 * 
	 * Create constructor. Primarily used for interactively constructing a new Partition Dimension.
	 * 
	 * Constructs an empty NodeGroup and Resource collection.
	 * 
	 * @param name
	 * @param columnType
	 */
	public PartitionDimension(String name, int columnType) {
		this(Hive.NEW_OBJECT_ID, name, columnType, new NodeGroup(new ArrayList<Node>()), null,
				new ArrayList<Resource>());
	}

	/**
	 * PERSISTENCE LOAD ONLY-- load a PartitionDimension from persistence.
	 * 
	 * @param id
	 * @param name
	 * @param columnType
	 * @param nodeGroup
	 * @param indexUri
	 * @param resources
	 */
	public PartitionDimension(int id, String name, int columnType,
			NodeGroup nodeGroup, String indexUri,
			Collection<Resource> resources) {
		super();
		this.id = id;
		this.name = name;
		this.columnType = columnType;
		this.nodeGroup = insetThisInstance(nodeGroup);
		this.indexUri = indexUri;
		this.resources = insetThisInstance(resources);
	}

	// Modify the passed in instance by setting its PartitionInstance
	private NodeGroup insetThisInstance(NodeGroup nodeGroup) {
		nodeGroup.setPartitionDimension(this);
		return nodeGroup;
	}

	private Collection<Resource> insetThisInstance(
			Collection<Resource> resources) {
		for (Resource resource : resources)
			resource.setPartitionDimension(this);
		return resources;
	}

	public int getColumnType() {
		return columnType;
	}

	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public NodeGroup getNodeGroup() {
		return nodeGroup;
	}
	@SuppressWarnings("unchecked")
	public<T extends Nameable> T findByName(Class<T> forClass, String name) throws HiveException {
		if (forClass.equals(Resource.class))
			return (T)getResource(name);
		if (forClass.equals(Node.class))
			return (T)getNodeGroup().getNode(name);
		throw new RuntimeException("Invalid type " + forClass.getName());
	}
	@SuppressWarnings("unchecked")
	public<T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if (forClass.equals(Resource.class))
			return (Collection<T>)getResources();
		if (forClass.equals(Node.class))
			return (Collection<T>)getNodeGroup().getNodes();
		throw new RuntimeException("Invalid type " + forClass.getName());
	}

	public void setNodeGroup(NodeGroup nodeGroup) {
		this.nodeGroup = nodeGroup;
	}

	public String getIndexUri() {
		return indexUri;
	}

	public void setIndexUri(String indexUri) {
		this.indexUri = indexUri;
	}

	public Collection<Resource> getResources() {
		return resources;
	}
	public Resource getResource(String resourceName) throws HiveException {
		for (Resource resource : resources)
			if (resource.getName().equals(resourceName))
				return resource;
		throw new HiveException("Resource with name " + resourceName + " not found.");
	}

	public Assigner getAssigner() {
		return assigner;
	}

	public void setAssigner(Assigner assigner) {
		this.assigner = assigner;
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
				name, columnType, nodeGroup, indexUri, resources
		});
	}
	
	public String toString()
	{
		String columnType = null;
		try {
			columnType = JdbcTypeMapper.jdbcTypeToString(getColumnType());
		}
		catch (Exception e)
		{
			columnType = "Error resolving column type: " + e.getMessage();
		}
		return HiveUtils.toDeepFormatedString(this, 
										"Id", 			getId(), 
										"Name", 		getName(), 
										"IndexUri", 	getIndexUri(),
										"ColumnType",	columnType,
										"NodeGroup",	getNodeGroup(),
										"Resources",	getResources());
	}
	
	public int compareTo(PartitionDimension o) {
		return getName().compareTo(o.getName());
	}
	
	public Object clone()
	{
		return new PartitionDimension(name, columnType);
	}
	public void setId(int id) {
		this.id = id;
	}
}
