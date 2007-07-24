/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.hivedb.Hive;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

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
	private Collection<Node> nodes;
	private String indexUri;
	private Collection<Resource> resources;

	private Assigner assigner = new Assigner() {
		private Random random = new Random(new Date().getTime());	
		public Node chooseNode(Collection<Node> nodes, Object value) {
			if (nodes.size()==0) throw new HiveRuntimeException("The Hive has no Nodes; the Assigner cannot make a choice.");
			return new ArrayList<Node>(nodes).get(random.nextInt(nodes.size()));
		}
		public Collection<Node> chooseNodes(Collection<Node> nodes, Object value) {
			return Arrays.asList(new Node[]{chooseNode(nodes, value)});
		}		
	};

	/**
	 * Create constructor
	 * 
	 * @param name
	 * @param columnType
	 * @param nodes
	 * @param indexUri The URI for the PartitionDimension's index tables.
	 *  Specify this if it is different then that of the hive.
	 * @param resources
	 * @param assigner
	 */
	public PartitionDimension(String name, int columnType, Collection<Node> nodes,
			String indexUri, Collection<Resource> resources) {
		this(Hive.NEW_OBJECT_ID, name, columnType, nodes, indexUri,
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
	 * @param nodes
	 * @param resources
	 */
	public PartitionDimension(String name, int columnType, Collection<Node> nodes,
			Collection<Resource> resources) {
		this(Hive.NEW_OBJECT_ID, name, columnType, nodes, null,
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
		this(Hive.NEW_OBJECT_ID, name, columnType, new ArrayList<Node>(), null,
				new ArrayList<Resource>());
	}

	/**
	 * PERSISTENCE LOAD ONLY-- load a PartitionDimension from persistence.
	 * 
	 * @param id
	 * @param name
	 * @param columnType
	 * @param nodes
	 * @param indexUri
	 * @param resources
	 */
	public PartitionDimension(int id, String name, int columnType,
			Collection<Node> nodes, String indexUri,
			Collection<Resource> resources) {
		super();
		this.id = id;
		this.name = name;
		this.columnType = columnType;
		this.nodes = insetNodes(nodes);
		this.indexUri = indexUri;
		this.resources = insetResources(resources);
	}

	// Modify the passed in instance by setting its PartitionInstance
	private Collection<Node> insetNodes(Collection<Node> nodes) {
		for(Node node: nodes)
			node.setPartitionDimension(this);
		return nodes;
	}

	private Collection<Resource> insetResources(
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

	@SuppressWarnings("unchecked")
	public<T extends Nameable> T findByName(Class<T> forClass, final String name){
		if (forClass.equals(Resource.class))
			return (T)getResource(name);
		if (forClass.equals(Node.class))
			return (T)getNode(name);
		
		throw new RuntimeException("Invalid type " + forClass.getName());
	}
	@SuppressWarnings("unchecked")
	public<T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if (forClass.equals(Resource.class))
			return (Collection<T>)getResources();
		if (forClass.equals(Node.class))
			return (Collection<T>)getNodes();
		throw new RuntimeException("Invalid type " + forClass.getName());
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
	public Resource getResource(String resourceName) {
		for (Resource resource : resources)
			if (resource.getName().equalsIgnoreCase(resourceName))
				return resource;
		throw new HiveKeyNotFoundException("Resource with name " + resourceName + " not found.", resourceName);
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
				name, columnType, nodes, indexUri, resources
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
	public Collection<Node> getNodes() {
		return nodes;
	}
	public Node getNode(final String name) {
		return Filter.grepSingle(new Predicate<Node>(){
			public boolean f(Node item) {
				return item.getName().equalsIgnoreCase(name);
			}}, getNodes());
	}
	public Node getNode(final int id) {
		return Filter.grepSingle(new Predicate<Node>(){
			public boolean f(Node item) {
				return item.getId() == id;
			}}, getNodes());
	}
	public void setNodes(Collection<Node> nodes) {
		this.nodes = nodes;
	}
}
