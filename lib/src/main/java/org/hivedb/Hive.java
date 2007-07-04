/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.hivedb.management.statistics.Counter;
import org.hivedb.management.statistics.DirectoryPerformanceStatistics;
import org.hivedb.management.statistics.DirectoryPerformanceStatisticsMBean;
import org.hivedb.management.statistics.HivePerformanceStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Directory;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.IdAndNameIdentifiable;
import org.hivedb.meta.Identifiable;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeSemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.DriverLoader;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.IdentifiableUtils;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Unary;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class Hive implements Synchronizeable, Observer {
	private static Logger log = Logger.getLogger(Hive.class);
	public static final int NEW_OBJECT_ID = 0;
	public static String URI_SYSTEM_PROPERTY = "org.hivedb.uri";
	private String hiveUri;
	private int revision;
	private boolean readOnly;
	private boolean performanceMonitoringEnabled = true;
	private Map<String,PartitionDimension> partitionDimensions;
	private PartitionKeyStatisticsDao partitionStatistics;
	private Map<String,Directory> directories;
	private DataSource dataSource;
	private Map<String, JdbcDaoSupportCacheImpl> jdbcDaoSupportCaches;
	private HivePerformanceStatistics performanceStatistics;
	
	/**
	 * System entry point. Factory method for all Hive interaction.
	 *
	 * @param hiveDatabaseUri
	 *            Target hive
	 * @return Hive (existing or new) located at hiveDatabaseUri
	 */
	public static Hive load(String hiveDatabaseUri) {
		log.debug("Loading Hive from " + hiveDatabaseUri);
		
		//Tickle driver
		try {
			DriverLoader.load(hiveDatabaseUri);
		} catch (ClassNotFoundException e) {
			throw new HiveRuntimeException("Unable to load database driver: " + e.getMessage(), e);
		} 
		
		Hive hive = new Hive(hiveDatabaseUri, 0, false, new ArrayList<PartitionDimension>());
		hive.sync();
		log.debug("Successfully loaded Hive from " + hiveDatabaseUri);

		return hive;
	}
	
	/***
	 * Alternate system entry point, using this load method enables runtime statistics tracking.
	 * Factory method for all Hive interaction. 
	 * 
	 * @param hiveDatabaseUri
	 * @param hiveStats
	 * @param directoryStats
	 * @return
	 */
	public static Hive load(String hiveDatabaseUri, HivePerformanceStatistics hiveStats, DirectoryPerformanceStatistics directoryStats) {
		Hive hive = Hive.load(hiveDatabaseUri);
		
		//Inject the statistics monitoring beans
		hive.setPerformanceStatistics(hiveStats);
		hive.setPerformanceMonitoringEnabled(true);
		
		for(Directory dir : hive.directories.values()){
			dir.setPerformanceStatistics((DirectoryPerformanceStatisticsMBean)directoryStats);
			dir.setPerformanceMonitoringEnabled(true);
		}
		return hive;
	}


	/**
	 * Explicitly syncs the hive with the persisted data.
	 *   
	 * 
	 */
	public void sync() {
		HiveSemaphore hs = new HiveSemaphoreDao(dataSource).get();
		setRevision(hs.getRevision());
		setReadOnly(hs.isReadOnly());
		
		//Reload partition dimensions
		Map<String, PartitionDimension> dimensionMap = new ConcurrentHashMap<String, PartitionDimension>();
		Map<String, Directory> directoryMap = new ConcurrentHashMap<String, Directory>();
		Map<String, JdbcDaoSupportCacheImpl> jdbcCacheMap = new ConcurrentHashMap<String, JdbcDaoSupportCacheImpl>();
		
		Counter directoryStats = null;
		if(directories.size() > 0)
			directoryStats = directories.values().iterator().next().getPerformanceStatistics();
		
		for (PartitionDimension p : new PartitionDimensionDao(dataSource).loadAll()){
			dimensionMap.put(p.getName(),p);
			if(isPerformanceMonitoringEnabled() && directoryStats != null)
				directoryMap.put(p.getName(), new Directory(p, new HiveBasicDataSource(p.getIndexUri()), directoryStats));
			else
				directoryMap.put(p.getName(), new Directory(p, new HiveBasicDataSource(p.getIndexUri())));
		}
		
		//Critical Section
		synchronized (this) {
			this.partitionDimensions = dimensionMap;
			this.directories = directoryMap;
		
			for(PartitionDimension p : this.getPartitionDimensions()) {
				if(isPerformanceMonitoringEnabled())
					jdbcCacheMap.put(p.getName(), new JdbcDaoSupportCacheImpl(p.getName(), this, directoryMap.get(p.getName()), performanceStatistics));
				else
					jdbcCacheMap.put(p.getName(), new JdbcDaoSupportCacheImpl(p.getName(), this, directoryMap.get(p.getName())));
			}
		
			this.jdbcDaoSupportCaches = jdbcCacheMap;
		}
	}

	/**
	 * INTERNAL USE ONLY- load the Hive from persistence.
	 * If you instantiate a Hive this way it will be in an invalid state.
	 * @param revision
	 * @param readOnly
	 */
	protected Hive(String hiveUri, int revision, boolean readOnly, Collection<PartitionDimension> partitionDimensions) {
		this.hiveUri = hiveUri;
		this.revision = revision;
		this.readOnly = readOnly;
		
		Map<String,PartitionDimension> dimensionNameMap = new ConcurrentHashMap<String, PartitionDimension>();
		for(PartitionDimension d : partitionDimensions)
			dimensionNameMap.put(d.getName(), d);
		this.partitionDimensions = dimensionNameMap;
		this.dataSource = new HiveBasicDataSource(this.getHiveUri());
		this.partitionStatistics = new PartitionKeyStatisticsDao(dataSource);
		this.jdbcDaoSupportCaches = new ConcurrentHashMap<String, JdbcDaoSupportCacheImpl>();
		this.directories = new ConcurrentHashMap<String, Directory>();
	}

	/**
	 * the URI of the hive database where all meta data is stored for this hive.
	 */
	public String getHiveUri() {
		return hiveUri;
	}

	/**
	 * A member of the HiveDbDialect enumeration corresponding to the underlying
	 * database type
	 */
	public HiveDbDialect getDialect() {
		return DriverLoader.discernDialect(hiveUri);
	}

	/**
	 * Hives are uniquely hashed by their URI, revision, partition dimensions,
	 * and read-only state
	 */
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, revision, getPartitionDimensions(), readOnly });
	}

	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	/**
	 * Indicates whether or not the hive metatables and indexes may be updated.
	 * 
	 * @return Returns true if the hive is in a read-only state.
	 */
	public boolean isReadOnly() {
		return readOnly;
	}

	/**
	 * INTERNAL USE ONLY - use updateHiveReadOnly to persist the hive's read
	 * only status Make the hive hive read-only, meaning hive metatables and
	 * indexes may not be updated.
	 * 
	 * @param readOnly
	 */
	protected void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	/***
	 * Set whether or not the Hive is read-only.
	 * @param readOnly true == read-only, false == read-write
	 */
	public void updateHiveReadOnly(Boolean readOnly) {
		this.setReadOnly(readOnly);
		new HiveSemaphoreDao(new HiveBasicDataSource(this.getHiveUri())).update(new HiveSemaphore(
				readOnly, this.getRevision()));
	}

	/***
	 * Set the read-only status of a particular node.
	 * @param node Target node
	 * @param readOnly true == read-only, false == read-write
	 * @throws HiveReadOnlyException
	 */
	public void updateNodeReadOnly(Node node, Boolean readOnly) throws HiveReadOnlyException{
		node.setReadOnly(readOnly);
		this.updateNode(node);
	}
	
	/**
	 * Get the current revision of the hive. The revision number is increased
	 * when new indexes are added to the hive or if the schema of an index is
	 * altered.
	 * 
	 * @return The current revision number of the hive.
	 */
	public int getRevision() {
		return revision;
	}

	/**
	 * INTERNAL USE ONLY - sets the current hive revision.
	 * 
	 * @param revision
	 */
	protected void setRevision(int revision) {
		this.revision = revision;
	}

	/**
	 * Gets all partition dimensions of the hive. A PartitionDimension instance
	 * references all of its underlying components--its NodeGroup and Resources.
	 * 
	 * @return
	 */
	public Collection<PartitionDimension> getPartitionDimensions() {
		return partitionDimensions.values();
	}
	
	/**
	 * Gets a partition dimension by name.
	 * 
	 * @param name
	 *            The user-defined name of a partition dimension
	 * @return
	 */
	public PartitionDimension getPartitionDimension(String name) {
		return partitionDimensions.get(name);
	}
	
	/**
	 * Test for existence of this partition dimension name
	 * @param name Name of partition dimension
	 * @return True if partition dimension exists
	 */
	public boolean containsPartitionDimension(String name) {
		return partitionDimensions.containsKey(name);
	}

	/**
	 * Adds a new partition dimension to the hive. The partition dimension
	 * persists to the database along with its NodeGroup, Nodes, Resources, and
	 * SecondaryIndexes. NodeGroup must be defined but the collections of Nodes,
	 * Resources, and SecondaryIndexes may be filled or empty, and modified
	 * later. If the partition dimension has a null indexUri it will be set to
	 * the URI of the hive.
	 * 
	 * @param partitionDimension
	 * @return The PartitionDimension with its Id set and those of all sub
	 *         objects.
	 */
	public PartitionDimension addPartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException {
		throwIfReadOnly("Creating a new partition dimension");
		throwIfNameIsNotUnique(String.format("Partition dimension %s already exists", partitionDimension.getName()), 
				getPartitionDimensions(),
				partitionDimension);
		
//		 We allow the partition dimension to not specify an indexUri and we default it to the hiveUri
		if(partitionDimension.getIndexUri() == null)
			partitionDimension.setIndexUri(this.hiveUri);
		HiveBasicDataSource pdDataSource = new HiveBasicDataSource(partitionDimension.getIndexUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(pdDataSource);	
		
		partitionDimensionDao.create(partitionDimension);
		this.directories.put(partitionDimension.getName(),new Directory(partitionDimension, pdDataSource));
		incrementAndPersistHive(this.dataSource);

		return partitionDimension;
	}

	/**
	 * Adds a node to the given partition dimension.
	 * 
	 * @param partitionDimension
	 *            A persisted partition dimension of the hive to which to add
	 *            the node
	 * @param node
	 *            A node instance initialized without an id and without a set
	 *            partition dimension
	 * @return The node with it's id set.
	 */
	public Node addNode(PartitionDimension partitionDimension, Node node)
			throws HiveReadOnlyException {
		
		node.setNodeGroup(partitionDimension.getNodeGroup());
		
		throwIfReadOnly("Creating a new node");
		throwIfNameIsNotUnique(String.format("Node with name %s already exists", node.getName()), 
				partitionDimension.getNodeGroup().getNodes(),
				node);
		
		NodeDao nodeDao = new NodeDao(dataSource);
		nodeDao.create(node);

		incrementAndPersistHive(dataSource);
		sync();
		return node;
	}

	/**
	 * 
	 * Adds a new resource to the given partition dimension, along with any
	 * secondary indexes defined in the resource instance
	 * 
	 * @param partitionDimension
	 *            A persisted partition dimensiono of the hive to which to add
	 *            the resource.
	 * @param resource
	 *            A resource instance initialized without an id and with a full
	 *            or empty collection of secondary indexes.
	 * @return The resource instance with its id set along with those of any
	 *         secondary indexes
	 */
	public Resource addResource(PartitionDimension partitionDimension,
			Resource resource) throws HiveReadOnlyException {
		resource.setPartitionDimension(partitionDimension);
		throwIfReadOnly("Creating a new resource");
		throwIfNameIsNotUnique(String.format(
				"Resource %s already exists in the partition dimension %s",
				resource.getName(), partitionDimension.getName()),
				partitionDimension.getResources(), resource);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		resourceDao.create(resource);
		incrementAndPersistHive(datasource);

		sync();
		return this.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName());
	}
	
	/**
	 * 
	 * Adds a new resource to the given partition dimension, along with any
	 * secondary indexes defined in the resource instance
	 * 
	 * @param dimensionName
	 *            The name of the persisted partition dimension of the hive to which to add
	 *            the resource.
	 * @param resource
	 *            A resource instance initialized without an id and with a full
	 *            or empty collection of secondary indexes.
	 * @return The resource instance with its id set along with those of any
	 *         secondary indexes
	 * @throws HiveReadOnlyException 
	 */
	public Resource addResource(String dimensionName,
			Resource resource) throws HiveReadOnlyException{
		return addResource(getPartitionDimension(dimensionName), resource);
	}

	/**
	 * 
	 * Adds a partition index to the given resource.
	 * 
	 * @param resource
	 *            A persited resource of a partition dimension of the hive to
	 *            which to add the secondary index
	 * @param secondaryIndex
	 *            A secondary index initialized without an id
	 * @return The SecondaryIndex instance with its id intialized
	 * @throws HiveReadOnlyException 
	 */
	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		secondaryIndex.setResource(resource);
		throwIfReadOnly("Creating a new secondary index");
		throwIfNameIsNotUnique(String.format(
				"Secondary index %s already exists in the resource %s",
				secondaryIndex.getName(), resource.getName()), resource
				.getSecondaryIndexes(), secondaryIndex);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(datasource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(datasource);
		sync();

		new IndexSchema(getPartitionDimension(resource.getPartitionDimension().getName())).install();
		return secondaryIndex;
	}

	/**
	 * Updates values of a partition dimension in the hive. No updates or adds
	 * to the underlying nodes, resources, or secondary indexes will persist.
	 * You must add or update these objects explicitly before calling this
	 * method. Any data of the partition dimension may be updated except its id.
	 * If new nodes, resources, or secondary indexes have been added to the
	 * partition dimension instance they will be persisted and assigned ids
	 * 
	 * @param partitionDimension
	 *            A partitionDimension persisted in the hive
	 * @return The partitionDimension passed in.
	 * @throws HiveReadOnlyException 
	 */
	public PartitionDimension updatePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException  {
		throwIfReadOnly("Updating partition dimension");
		throwIfIdNotPresent(String.format(
				"Partition dimension with id %s does not exist",
				partitionDimension.getId()), getPartitionDimensions(),
				partitionDimension);
		throwIfNameIsNotUnique(String.format(
				"Partition dimension with name %s already exists",
				partitionDimension.getName()), getPartitionDimensions(),
				partitionDimension);

		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(
				datasource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(datasource);
		sync();
		
		return partitionDimension;
	}

	/**
	 * Updates the values of a node.
	 * 
	 * @param node
	 *            A node instance initialized without an id and without a set
	 *            partition dimension
	 * @return The node with it's id set.
	 */
	public Node updateNode(Node node) throws HiveReadOnlyException {
		throwIfReadOnly("Updating node");
		throwIfIdNotPresent(String.format("Node with id %s does not exist",
				node.getName()), node.getNodeGroup().getNodes(), node);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		NodeDao nodeDao = new NodeDao(datasource);
		nodeDao.update(node);

		incrementAndPersistHive(datasource);

		sync();
		return node;
	}

	/**
	 * 
	 * Updates resource. No secondary index data is created or updated. You
	 * should explicitly create or update any modified secondary index data in
	 * the resource before calling this method.
	 * 
	 * @param resource
	 *            A resource belonging to a partition dimension of the hive
	 * @return The resource instance passed in
	 * @throws HiveReadOnlyException 
	 */
	public Resource updateResource(Resource resource) throws HiveReadOnlyException  {
		throwIfReadOnly("Updating resource");
		throwIfIdNotPresent(String.format(
				"Resource with id %s does not exist", resource.getId()),
				resource.getPartitionDimension().getResources(), resource);
		throwIfNameIsNotUnique(String.format("Resource with name %s already exists", resource
				.getName()), resource.getPartitionDimension().getResources(),
				resource);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		resourceDao.update(resource);
		incrementAndPersistHive(datasource);

		sync();
		return resource;
	}

	/**
	 * 
	 * Adds a partition index to the given resource.
	 * 
	 * @param resource
	 *            A persited resource of a partition dimension of the hive to
	 *            which to add the secondary index
	 * @param secondaryIndex
	 *            A secondary index initialized without an id
	 * @return The SecondaryIndex instance with its id intialized
	 * @throws HiveReadOnlyException 
	 */
	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException  {
		throwIfReadOnly("Updating secondary index");
		throwIfIdNotPresent(String.format(
				"Secondary index with id %s does not exist", secondaryIndex
						.getId()), secondaryIndex.getResource()
				.getSecondaryIndexes(), secondaryIndex);
		throwIfNameIsNotUnique(String.format("Secondary index with name %s already exists",
				secondaryIndex.getName()), secondaryIndex.getResource()
				.getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(dataSource);
		secondaryIndexDao.update(secondaryIndex);
		incrementAndPersistHive(dataSource);

		sync();
		return secondaryIndex;
	}

	/***
	 * Remove a partition dimension from the hive.
	 * @param partitionDimension
	 * @return
	 * @throws HiveReadOnlyException 
	 */
	public PartitionDimension deletePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException {
		throwIfReadOnly(String.format("Deleting partition dimension %s",
				partitionDimension.getName()));
		throwUnlessItemExists(
				String
						.format(
								"Partition dimension %s does not match any partition dimension in the hive",
								partitionDimension.getName()),
				getPartitionDimensions(), partitionDimension);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(datasource);
		partitionDimensionDao.delete(partitionDimension);
		incrementAndPersistHive(datasource);
		sync();
		
		//Destroy the corresponding DataSourceCache
		this.jdbcDaoSupportCaches.remove(partitionDimension.getName());
		
		return partitionDimension;
	}

	/***
	 * remove a node from the hive.
	 * @param node
	 * @return
	 * @throws HiveReadOnlyException 
	 */
	public Node deleteNode(Node node) throws HiveReadOnlyException {
		throwIfReadOnly(String.format("Deleting node %s", node.getName()));
		throwUnlessItemExists(
				String
						.format(
								"Node %s does not match any node in the partition dimenesion %s",
								node.getName(), node.getNodeGroup()
										.getPartitionDimension().getName()),
				node.getNodeGroup().getPartitionDimension().getNodeGroup()
						.getNodes(), node);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		NodeDao nodeDao = new NodeDao(datasource);
		nodeDao.delete(node);
		incrementAndPersistHive(datasource);
		sync();
		
		//Synchronize the DataSourceCache
		this.jdbcDaoSupportCaches.get(node.getNodeGroup().getPartitionDimension().getName()).sync();
		return node;
	}

	/***
	 * remove a resource.
	 * @param resource
	 * @return
	 * @throws HiveReadOnlyException
	 */
	public Resource deleteResource(Resource resource) throws HiveReadOnlyException {
		throwIfReadOnly(String.format("Deleting resource %s", resource.getName()));
		throwUnlessItemExists(
				String
						.format(
								"Resource %s does not match any resource in the partition dimenesion %s",
								resource.getName(), resource
										.getPartitionDimension().getName()),
				resource.getPartitionDimension().getResources(), resource);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		resourceDao.delete(resource);
		incrementAndPersistHive(datasource);
		sync();
		
		return resource;
	}

	/***
	 * Remove a secondary index.
	 * @param secondaryIndex
	 * @return
	 * @throws HiveReadOnlyException 
	 */
	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		throwIfReadOnly(String.format("Deleting secondary index %s", secondaryIndex
				.getName()));
		throwUnlessItemExists(
				String
						.format(
								"Secondary index %s does not match any node in the resource %s",
								secondaryIndex.getName(), secondaryIndex
										.getResource()), secondaryIndex
						.getResource().getSecondaryIndexes(), secondaryIndex);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(datasource);
		secondaryindexDao.delete(secondaryIndex);
		incrementAndPersistHive(datasource);
		sync();
		
		return secondaryIndex;
	}

	private void incrementAndPersistHive(DataSource datasource) {
		new HiveSemaphoreDao(datasource).incrementAndPersist();
		this.sync();
	}

	/**
	 * Inserts a new primary index key into the given partition dimension. A
	 * partition dimension by definition defines one primary index. The given
	 * primaryIndexKey must match the column type defined in
	 * partitionDimenion.getColumnInfo().getColumnType(). The node used for the
	 * new primary index key is determined by the hive's
	 * 
	 * @param partitionDimension -
	 *            an existing partition dimension of the hive.
	 * @param primaryIndexKey -
	 *            a primary index key not yet in the primary index.
	 * @throws HiveReadOnlyException 
	 * @throws SQLException
	 *             Throws if the primary index key already exists, or another
	 *             persitence error occurs.
	 */
	public void insertPrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveReadOnlyException {
		// TODO: Consider redesign of NodeGroup to perform assignment, or at
		// least provider direct iteration over Nodes
		Node node = partitionDimension.getAssigner().chooseNode(
				partitionDimension.getNodeGroup().getNodes(), primaryIndexKey);
		throwIfReadOnly("Inserting a new primary index key", node);
		directories.get(partitionDimension.getName()).insertPrimaryIndexKey(node,
				primaryIndexKey);
	}

	/**
	 * Inserts a new primary index key into the given partition dimension. A
	 * partition dimension by definition defines one primary index. The given
	 * primaryIndexKey must match the column type defined in
	 * partitionDimenion.getColumnType().
	 * 
	 * @param partitionDimensionName -
	 *            the name of an existing partition dimension of the hive.
	 * @param primaryIndexKey -
	 *            a primary index key not yet in the primary index.
	 * @throws HiveReadOnlyException 
	 * @throws HiveException
	 *             Throws if the partition dimension is not in the hive, or if
	 *             the hive, primary index or node is currently read only.
	 */
	public void insertPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveReadOnlyException {
		insertPrimaryIndexKey(getPartitionDimension(partitionDimensionName),
				primaryIndexKey);
	}

	/**
	 * Inserts a new secondary index key and the primary index key which it
	 * references into the given secondary index.
	 * 
	 * @param secondaryIndex
	 *            A secondary index which belongs to the hive via a resource and
	 *            partition dimension
	 * @param secondaryIndexKey
	 *            A secondary index key value whose type must match that defined
	 *            by secondaryIndex.getColumnInfo().getColumnType()
	 * @param primaryindexKey
	 *            A primary index key that already exists in the primary index
	 *            of the partition dimension of this secondary index.
	 * @throws HiveReadOnlyException 
	 */
	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object primaryIndexKey) throws HiveReadOnlyException {
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		boolean primaryKeyReadOnly = getReadOnlyOfPrimaryIndexKey(getPartitionDimension(partitionDimensionName), primaryIndexKey);
		for(Integer id : directories.get(partitionDimensionName).getNodeIdsOfPrimaryIndexKey(primaryIndexKey))
			throwIfReadOnly("Inserting a new secondary index key", getPartitionDimension(partitionDimensionName).getNodeGroup().getNode(id), primaryIndexKey, primaryKeyReadOnly);

		throwIfReadOnly("Inserting a new secondary index key");
		directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						primaryIndexKey);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource()
				.getPartitionDimension(), primaryIndexKey, 1);
	}

	/**
	 * 
	 * Inserts a new secondary index key and the primary index key which it
	 * references into the secondary index identified by the give
	 * secondaryIndexName, resourceName, and partitionDimensionName
	 * 
	 * @param partitionDimensionName -
	 *            the name of a partition dimension in the hive
	 * @param resourceName -
	 *            the name of a resource in the partition dimension
	 * @param secondaryIndexName -
	 *            the name of a secondary index in the resource
	 * @param secondaryIndexKey
	 *            A secondary index key value whose type must match that defined
	 *            by secondaryIndex.getColumnInfo().getColumnType()
	 * @param primaryindexKey
	 *            A primary index key that already exists in the primary index
	 *            of the partition dimension of this secondary index.
	 * @throws HiveReadOnlyException
	 */
	public void insertSecondaryIndexKey(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey, Object primaryIndexKey) throws HiveReadOnlyException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				primaryIndexKey);
	}

	/**
	 * 
	 * Updates the read-only status of the given primary index key for the given
	 * partition dimension.
	 * 
	 * @param partitionDimension
	 *            A partition dimension of the hive
	 * @param primaryIndexKey
	 *            An existing primary index key in the primary index
	 * @param isReadOnly
	 *            True makes the primary index key rean-only, false makes it
	 *            writable
	 */
	public void updatePrimaryIndexReadOnly(
			PartitionDimension partitionDimension, Object primaryIndexKey,
			boolean isReadOnly) throws HiveReadOnlyException {
		// This query validates the existence of the primaryIndexKey
		getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey);
		directories.get(partitionDimension.getName()).updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
	}

	/**
	 * 
	 * Updates the read-only status of the given primary index key for the given
	 * partition dimension.
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension of the hive
	 * @param primaryIndexKey
	 *            An existing primary index key in the primary index
	 * @param isReadOnly
	 *            True makes the primary index key rean-only, false makes it
	 *            writable
	 * @throws HiveReadOnlyException 
	 */
	public void updatePrimaryIndexReadOnly(String partitionDimensionName,
			Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		updatePrimaryIndexReadOnly(partitionDimension, primaryIndexKey,
				isReadOnly);
	}

	/**
	 * 
	 * Updates the primary index key of the given secondary index key.
	 * 
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via a resource and
	 *            partition dimension
	 * @param secondaryIndexKey
	 *            A secondary index key of the given secondary index
	 * @param primaryIndexKey
	 *            The primary index key to assign to the secondary index key
	 * @throws HiveReadOnlyException 
	 */
	public void updatePrimaryIndexKeyOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey,
			Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		
		throwIfReadOnly("Updating primary index key of secondary index key");
		directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
				.updatePrimaryIndexOfSecondaryKey(secondaryIndex,
						secondaryIndexKey, originalPrimaryIndexKey, newPrimaryIndexKey);
	}

	/**
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in this hive
	 * @param resourceName
	 *            The name of a resource in the given partition dimension
	 * @param secondaryIndexName
	 *            The name of a secondary index of the given resource
	 * @param secondaryIndexKey
	 *            A secondary index key of the given secondary index
	 * @param primaryIndexKey
	 *            The primary index key to assign to the secondary index key
	 * @throws HiveReadOnlyException 
	 */
	public void updatePrimaryIndexKeyOfSecondaryIndexKey(
			String partitionDimensionName, String resourceName,
			String secondaryIndexName, Object secondaryIndexKey,
			Object originalPrimaryIndexKey,  Object newPrimaryIndexKey) throws HiveReadOnlyException {
		updatePrimaryIndexKeyOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				originalPrimaryIndexKey, newPrimaryIndexKey);
	}

	/**
	 * Deletes the primary index key of the given partition dimension
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 */
	public void deletePrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveReadOnlyException {

		if (!doesPrimaryIndexKeyExist(partitionDimension, primaryIndexKey))
			throw new HiveKeyNotFoundException("The primary index key " + primaryIndexKey
					+ " does not exist",primaryIndexKey);
		
		for(NodeSemaphore node: getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey)){
			throwIfReadOnly("Deleting primary index key", partitionDimension.getNodeGroup().getNode(node.getId()), primaryIndexKey,
					getReadOnlyOfPrimaryIndexKey(partitionDimension,
							primaryIndexKey));
		}
		

		for (Resource resource : partitionDimension.getResources())
			for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {
				directories.get(partitionDimension.getName())
						.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(
								secondaryIndex, primaryIndexKey);
			}

		directories.get(partitionDimension.getName()).deletePrimaryIndexKey(primaryIndexKey);
	}

	/**
	 * Deletes a primary index key of the given partition dimension
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @throws HiveReadOnlyException 
	 */
	public void deletePrimaryIndexKey(String partitionDimensionName,
			Object secondaryIndexKey) throws HiveReadOnlyException{
		deletePrimaryIndexKey(getPartitionDimension(partitionDimensionName),
				secondaryIndexKey);
	}

	/**
	 * Deletes a secondary index key of the give secondary index
	 * 
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via its resource
	 *            and partition dimension
	 * @param secondaryIndexKey
	 *            An existing secondary index key
	 * @throws HiveReadOnlyException 
	 * @throws HiveException
	 *             Throws if the secondary index key does not exist or if the
	 *             hive is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object primaryIndexKey) throws HiveReadOnlyException{
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		boolean primaryKeyReadOnly = getReadOnlyOfPrimaryIndexKey(getPartitionDimension(partitionDimensionName), primaryIndexKey);
		for(Integer id : directories.get(partitionDimensionName).getNodeIdsOfPrimaryIndexKey(primaryIndexKey))
			throwIfReadOnly("Deleting secondary index key", getPartitionDimension(partitionDimensionName).getNodeGroup().getNode(id), primaryIndexKey, primaryKeyReadOnly);
		if (!doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey))
			throw new HiveKeyNotFoundException("Secondary index key "
					+ secondaryIndexKey.toString() + " does not exist",secondaryIndexKey);

		directories.get(partitionDimensionName)
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, primaryIndexKey);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource()
				.getPartitionDimension(), primaryIndexKey, 1);
	}

	/**
	 * Deletes a secondary index key of the give secondary index
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param resourceName
	 *            The name of a resource in the partition dimension
	 * @param secondaryIndexName
	 *            The name of a secondary index of the resource
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via its resource
	 *            and partition dimension
	 * @param secondaryIndexKey
	 *            An existing secondary index key
	 * @throws HiveReadOnlyException 
	 * @throws HiveException
	 *             Throws if the secondary index key does not exist or if the
	 *             hive is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deleteSecondaryIndexKey(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey, Object primaryIndexKey) throws HiveReadOnlyException {
		deleteSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey, primaryIndexKey);
	}

	/**
	 * Returns true if the primary index key exists in the given partition
	 * dimension
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            The key to test
	 * @return
	 * @throws HiveException
	 *             Throws if the partition dimension is not in the hive
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public boolean doesPrimaryIndexKeyExist(
			PartitionDimension partitionDimension, Object primaryIndexKey) {
		return directories.get(partitionDimension.getName()).doesPrimaryIndexKeyExist(
				primaryIndexKey);
	}

	/**
	 * Returns true if the primary index key exists in the given partition
	 * dimension
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            The key to test
	 * @return
	 * @throws HiveException
	 *             Throws if the partition dimension is not in the hive

	 */
	public boolean doesPrimaryIndeyKeyExist(String partitionDimensionName,
			Object primaryIndexKey) {
		return doesPrimaryIndexKeyExist(
				getPartitionDimension(partitionDimensionName), primaryIndexKey);
	}

	/**
	 * Returns the node assigned to the given primary index key
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            A primary index key belonging to the partition dimension
	 * @return

	 */
	private Collection<NodeSemaphore> getNodeSemaphoresOfPrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) {
		return directories.get(partitionDimension.getName()).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey);
	}
	
	/**
	 * Returns true is the given primary index key is read-only
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @return
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(
			PartitionDimension partitionDimension, Object primaryIndexKey) {
		Boolean readOnly = directories.get(partitionDimension.getName())
				.getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
		if (readOnly != null)
			return readOnly;
		throw new HiveKeyNotFoundException(String.format(
				"Primary index key %s of partition dimension %s not found.",
				primaryIndexKey.toString(), partitionDimension.getName()),primaryIndexKey);
	}

	/**
	 * Returns true is the given primary index key is read-only
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @return
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) {
		return getReadOnlyOfPrimaryIndexKey(getPartitionDimension(partitionDimensionName), primaryIndexKey);
	}

	/**
	 * Tests the existence of a give secondary index key
	 * 
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via its resource
	 *            and partition index
	 * @param secondaryIndexKey
	 *            The key to test
	 * @return True if the secondary index key exists
	 */
	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) {
		return directories.get(
				secondaryIndex.getResource().getPartitionDimension().getName())
				.doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey);
	}

	/**
	 * 
	 * Tests the existence of a give secondary index key
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param resourceName
	 *            The name of a resource in the partition dimesnion
	 * @param secondaryIndexName
	 *            The name of a secondary index of the resource
	 * @param secondaryIndexKey
	 *            The key of the secondary index to test
	 * @return True if the key exists in the secondary index
	 */
	public boolean doesSecondaryIndexKeyExist(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey) {
		return doesSecondaryIndexKeyExist(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey);
	}
	
	private Collection<NodeSemaphore> getNodeSemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) {
		return directories.get(secondaryIndex.getResource().getPartitionDimension().getName()).getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
	}

	/**
	 * Returns the primary index key of the given secondary index key
	 * 
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via its resource
	 *            and partition dimension
	 * @param secondaryIndexKey
	 *            The secondary in
	 * @return
	 */
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		PartitionDimension partitionDimension = secondaryIndex.getResource()
				.getPartitionDimension();
		Collection<Object> primaryIndexKeys = directories.get(partitionDimension.getName())
				.getPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex,
						secondaryIndexKey);
		if (primaryIndexKeys.size() > 0)
			return primaryIndexKeys;
		else
			throw new HiveKeyNotFoundException(
				String
						.format(
								"Secondary index key %s of partition dimension %s on secondary index %s not found.",
								secondaryIndex.toString(), partitionDimension
										.getName(), secondaryIndex.getName()), primaryIndexKeys);
	}

	/**
	 * Returns all secondary index keys pertaining to the given primary index
	 * key. The primary index key may or may not exist in the primary index and
	 * there may be zero or more keys returned.
	 * 
	 * @param secondaryIndex
	 *            the secondary index to query
	 * @param primaryIndexKey
	 *            the primary index key with which to query
	 * @return
	 */
	public Collection getSecondaryIndexKeysWithPrimaryKey(
			SecondaryIndex secondaryIndex, Object primaryIndexKey) {
		return directories.get(
				secondaryIndex.getResource().getPartitionDimension().getName())
				.getSecondaryIndexKeysOfPrimaryIndexKey(secondaryIndex,
						primaryIndexKey);
	}

	/**
	 * 
	 * Returns all secondary index keys pertaining to the given primary index
	 * key. The primary index key must exist in the primary index and there may
	 * be zero or more keys returned.
	 * 
	 * @param partitionDimensionName
	 * @param resource
	 * @param secondaryIndexName
	 * @param primaryIndexKey
	 * @return
	 */
	public Collection getSecondaryIndexKeysWithPrimaryKey(
			String partitionDimensionName, String resource,
			String secondaryIndexName, Object primaryIndexKey) {
		return getSecondaryIndexKeysWithPrimaryKey(getPartitionDimension(
				partitionDimensionName).getResource(resource)
				.getSecondaryIndex(secondaryIndexName), primaryIndexKey);
	}
	
	private Connection getConnection(PartitionDimension partitionDimension, NodeSemaphore semaphore, AccessType intention) throws HiveReadOnlyException,SQLException {
		try{
			if( intention == AccessType.ReadWrite && isKeyReadOnly(partitionDimension, semaphore))
				throw new HiveReadOnlyException("The key/node/hive requested cannot be written to at this time.");
			
			Connection conn = DriverManager.getConnection(partitionDimension.getNodeGroup().getNode(semaphore.getId()).getUri());
			if(intention == AccessType.Read) {
				conn.setReadOnly(true);
				if( isPerformanceMonitoringEnabled() )
					performanceStatistics.incrementNewReadConnections();
			} else if( intention == AccessType.ReadWrite){
				if( isPerformanceMonitoringEnabled() )
					performanceStatistics.incrementNewWriteConnections();
			}
			return conn;
		} catch (SQLException e) {
			if( isPerformanceMonitoringEnabled() )
				performanceStatistics.incrementConnectionFailures();
			throw e;
		} catch( RuntimeException e) {
			if( isPerformanceMonitoringEnabled() )
				performanceStatistics.incrementConnectionFailures();
			throw e;
		} catch(HiveReadOnlyException e) {
			if( isPerformanceMonitoringEnabled() )
				performanceStatistics.incrementConnectionFailures();
			throw e;
		}
	}
	
	private boolean isKeyReadOnly(PartitionDimension partitionDimension, NodeSemaphore semaphore) {
		return isReadOnly() || partitionDimension.getNodeGroup().getNode(semaphore.getId()).isReadOnly() || semaphore.isReadOnly();
	}
	
	/***
	 * Get a JDBC connection to a data node by partition key.
	 * @param partitionDimension
	 * @param primaryIndexKey
	 * @param intent
	 * @return
	 * @throws HiveReadOnlyException 
	 */
	public Collection<Connection> getConnection(PartitionDimension partitionDimension,
			Object primaryIndexKey, AccessType intent) throws SQLException, HiveReadOnlyException {
		Collection<Connection> connections = new ArrayList<Connection>();
		for(NodeSemaphore semaphore : getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey))
			connections.add(getConnection(partitionDimension, semaphore, intent));
		return connections;
	}
	
	/***
	 * Get a JDBC connection to a data node by a secondary index key.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param intent
	 * @return
	 * @throws HiveException
	 * @throws SQLException
	 */
	public Collection<Connection> getConnection(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, AccessType intent) throws HiveReadOnlyException, SQLException {
		if(AccessType.ReadWrite == intent)
			throw new UnsupportedOperationException("Writes must be performed using the primary index key.");
		
		Collection<Connection> connections = new ArrayList<Connection>();
		Collection<NodeSemaphore> nodeSemaphores = getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		nodeSemaphores = Filter.getUnique(nodeSemaphores, new Unary<NodeSemaphore, Integer>(){
			public Integer f(NodeSemaphore item) {
				return item.getId();
			}});
		for(NodeSemaphore semaphore : nodeSemaphores)
			connections.add(getConnection(secondaryIndex.getResource().getPartitionDimension(), semaphore, intent));
		return connections;
	}
	
	/***
	 * Get a JdbcDaoSupportCache for a partition dimension.
	 * @param partitionDimension
	 * @return
	 */
	public JdbcDaoSupportCache getJdbcDaoSupportCache(PartitionDimension partitionDimension) {
		return this.getJdbcDaoSupportCache(partitionDimension.getName());
	}
	/***
	 * Get a JdbcDaoSupportCache for a partition dimension by name.
	 * @param partitionDimension
	 * @return
	 */
	public JdbcDaoSupportCache getJdbcDaoSupportCache(String partitionDimensionName) {
		return this.jdbcDaoSupportCaches.get(partitionDimensionName);
	}

	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getHiveUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimensions());
	}

	@SuppressWarnings("unchecked")
	private <T extends IdAndNameIdentifiable> void throwIfNameIsNotUnique(
			String errorMessage, Collection<T> collection, T item) {
		// Forbids duplicate names for two different instances if the class
		// implements Identifies
		if(!IdentifiableUtils.isNameUnique((Collection<IdAndNameIdentifiable>) collection, item))
				throw new HiveRuntimeException(errorMessage);
	}

	@SuppressWarnings("unchecked")
	private <T extends Identifiable> void throwIfIdNotPresent(
			String errorMessage, Collection<T> collection, T item) {
		if(!IdentifiableUtils.isIdPresent((Collection<IdAndNameIdentifiable>)collection, (IdAndNameIdentifiable) item))
			throw new HiveKeyNotFoundException(errorMessage, item);
	}

	private <T> void throwUnlessItemExists(String errorMessage,
			Collection<T> collection, T item) {
		// All classes implement Comparable, so this does a deep compare on all
		// objects owned by the item.
		if (!collection.contains(item))
			throw new HiveKeyNotFoundException(errorMessage, item);
	}

	private void throwIfReadOnly(String errorMessage) throws HiveReadOnlyException {
		if (this.isReadOnly())
			throw new HiveReadOnlyException(
					errorMessage
							+ ". This operation is invalid because the hive is currently read-only.");
	}

	private void throwIfReadOnly(String errorMessage, Node node) throws HiveReadOnlyException {
		throwIfReadOnly(errorMessage);
		if (node.isReadOnly())
			throw new HiveReadOnlyException(errorMessage
					+ ". This operation is invalid becuase the selected node "
					+ node.getId() + " is currently read-only.");
	}
	
	@SuppressWarnings("unused")
	private void throwIfReadOnly(String errorMessage, Collection<Node> nodes) throws HiveReadOnlyException {
		for(Node node : nodes)
			throwIfReadOnly(errorMessage, node);
	}

	private void throwIfReadOnly(String errorMessage, Node node,
			Object primaryIndexKeyId, boolean primaryIndexKeyReadOnly)
			throws HiveReadOnlyException {
		throwIfReadOnly(errorMessage, node);
		if (primaryIndexKeyReadOnly)
			throw new HiveReadOnlyException(
					errorMessage
							+ ". This operation is invalid becuase the primary index key "
							+ primaryIndexKeyId.toString()
							+ " is currently read-only.");
	}

	public PartitionKeyStatisticsDao getPartitionStatistics() {
		return partitionStatistics;
	}

	public HivePerformanceStatistics getPerformanceStatistics() {
		return performanceStatistics;
	}

	public void setPerformanceStatistics(HivePerformanceStatistics performanceStatistics) {
		this.performanceStatistics = performanceStatistics;
	}

	public boolean isPerformanceMonitoringEnabled() {
		return performanceStatistics != null && performanceMonitoringEnabled;
	}

	public void setPerformanceMonitoringEnabled(boolean performanceMonitoringEnabled) {
		this.performanceMonitoringEnabled = performanceMonitoringEnabled;
	}

	public void update(Observable o, Object arg) {
		sync();
	}

}
