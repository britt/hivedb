/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

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
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.meta.persistence.HiveBasicDataSourceProvider;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.DriverLoader;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.IdentifiableUtils;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class Hive extends Observable implements Synchronizeable, Observer {
	//logger
	private static Logger log = Logger.getLogger(Hive.class);
	//constants
	private static final int DEFAULT_JDBC_TIMEOUT = 500;
	public static final int NEW_OBJECT_ID = 0;
	
	private String hiveUri;
	private int revision;
	private boolean readOnly;
	private boolean performanceMonitoringEnabled = true;
	private Map<String,PartitionDimension> partitionDimensions;
	private PartitionKeyStatisticsDao partitionStatistics;
	private Map<String,Directory> directories;
	private Map<String, JdbcDaoSupportCacheImpl> jdbcDaoSupportCaches;
	private HivePerformanceStatistics performanceStatistics;
	private DataSource hiveDataSource;
	private Map<String, DataSource> nodeDataSources;
	private DataSourceProvider dataSourceProvider;
	
	/**
	 * System entry point. Factory method for all Hive interaction.
	 *
	 * @param hiveDatabaseUri
	 *            Target hive
	 * @return Hive (existing or new) located at hiveDatabaseUri
	 */
	public static Hive load(String hiveDatabaseUri) {
		return Hive.load(hiveDatabaseUri, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT), null, null);
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
	public static Hive load(String hiveDatabaseUri, DataSourceProvider dataSourceProvider, HivePerformanceStatistics hiveStats, DirectoryPerformanceStatistics directoryStats) {
		log.debug("Loading Hive from " + hiveDatabaseUri);
		
		//Tickle driver
		try {
			DriverLoader.load(hiveDatabaseUri);
		} catch (ClassNotFoundException e) {
			throw new HiveRuntimeException("Unable to load database driver: " + e.getMessage(), e);
		} 
		
		Hive hive = new Hive(hiveDatabaseUri, 0, false, new ArrayList<PartitionDimension>(), dataSourceProvider);
		hive.sync();
		log.debug("Successfully loaded Hive from " + hiveDatabaseUri);
		
		//Inject the statistics monitoring beans
		if( hiveStats != null) {
			hive.setPerformanceStatistics(hiveStats);
			hive.setPerformanceMonitoringEnabled(true);
		} 
		
		if( directoryStats != null) {
			for(Directory dir : hive.directories.values()){
				dir.setPerformanceStatistics((DirectoryPerformanceStatisticsMBean)directoryStats);
				dir.setPerformanceMonitoringEnabled(true);
			}
		}
		return hive;
	}


	/**
	 * Explicitly syncs the hive with the persisted data.
	 *   
	 * 
	 */
	public void sync() {
		
		HiveSemaphore hs = new HiveSemaphoreDao(hiveDataSource).get();
		this.revision = hs.getRevision();
		this.readOnly = hs.isReadOnly();
		
		//Reload partition dimensions
		Map<String, PartitionDimension> dimensionMap = new ConcurrentHashMap<String, PartitionDimension>();
		Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<String, DataSource>();
		Map<String, Directory> directoryMap = new ConcurrentHashMap<String, Directory>();
		Map<String, JdbcDaoSupportCacheImpl> jdbcCacheMap = new ConcurrentHashMap<String, JdbcDaoSupportCacheImpl>();
		
		Counter directoryStats = null;
		if(directories.size() > 0)
			directoryStats = directories.values().iterator().next().getPerformanceStatistics();
		
		for (PartitionDimension p : new PartitionDimensionDao(hiveDataSource).loadAll()){
			dimensionMap.put(p.getName(),p);
			
			for(Node node : p.getNodes())
				if(!dataSourceMap.containsKey(node.getUri()))
					dataSourceMap.put(node.getUri(), dataSourceProvider.getDataSource(node));
			
			if(isPerformanceMonitoringEnabled() && directoryStats != null)
				directoryMap.put(p.getName(), new Directory(p, dataSourceProvider.getDataSource(p.getIndexUri()), directoryStats));
			else
				directoryMap.put(p.getName(), new Directory(p, dataSourceProvider.getDataSource(p.getIndexUri())));
		}
		
		//Critical Section
		synchronized (this) {
			this.partitionDimensions = dimensionMap;
			this.directories = directoryMap;
		
			for(PartitionDimension p : this.getPartitionDimensions()) {
				if(isPerformanceMonitoringEnabled())
					jdbcCacheMap.put(p.getName(), new JdbcDaoSupportCacheImpl(p.getName(), this, directoryMap.get(p.getName()), dataSourceProvider, performanceStatistics));
				else
					jdbcCacheMap.put(p.getName(), new JdbcDaoSupportCacheImpl(p.getName(), this, directoryMap.get(p.getName()), dataSourceProvider));
			}
			this.nodeDataSources = dataSourceMap;
			this.jdbcDaoSupportCaches = jdbcCacheMap;
		}
	}

	/**
	 * INTERNAL USE ONLY- load the Hive from persistence.
	 * If you instantiate a Hive this way it will be in an invalid state.
	 * @param revision
	 * @param readOnly
	 */
	protected Hive(String hiveUri, int revision, boolean readOnly, Collection<PartitionDimension> partitionDimensions, DataSourceProvider dataSourceProvider) {
		this.hiveUri = hiveUri;
		this.revision = revision;
		this.readOnly = readOnly;
		
		this.dataSourceProvider = dataSourceProvider;
		this.hiveDataSource = dataSourceProvider.getDataSource(hiveUri);
		
		Map<String,PartitionDimension> dimensionNameMap = new ConcurrentHashMap<String, PartitionDimension>();
		this.nodeDataSources = new ConcurrentHashMap<String, DataSource>();
		
		for(PartitionDimension d : partitionDimensions) {
			dimensionNameMap.put(d.getName(), d);
			for(Node node : d.getNodes())
				if(!nodeDataSources.containsKey(node.getUri()))
					nodeDataSources.put(node.getUri(), dataSourceProvider.getDataSource(node));
		}
		this.partitionDimensions = dimensionNameMap;
		this.partitionStatistics = new PartitionKeyStatisticsDao(hiveDataSource);
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

	/***
	 * Set whether or not the Hive is read-only.
	 * @param readOnly true == read-only, false == read-write
	 */
	public void updateHiveReadOnly(Boolean readOnly) {
		this.readOnly = readOnly;
		new HiveSemaphoreDao(hiveDataSource).update(new HiveSemaphore(
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
		if (!partitionDimensions.containsKey(name))
			throw new HiveRuntimeException(String.format("Unknown partition dimension %s", name));
		return partitionDimensions.get(name);
	}
	
	/**
	 * Gets a partition dimension by id.
	 * 
	 * @param name
	 *            The user-defined name of a partition dimension
	 * @return
	 */
	public PartitionDimension getPartitionDimension(final int id) {
		return Filter.grepSingle(new Predicate<PartitionDimension>(){

			public boolean f(PartitionDimension item) {
				return item.getId() == id;
			}}, partitionDimensions.values());
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
		
		DataSource pdDataSource = dataSourceProvider.getDataSource(partitionDimension.getIndexUri());
		new PartitionDimensionDao(pdDataSource).create(partitionDimension);
		this.directories.put(partitionDimension.getName(),new Directory(partitionDimension, pdDataSource));
		incrementAndPersistHive(hiveDataSource);
		new IndexSchema(partitionDimension).install();
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
		
		node.setPartitionDimension(partitionDimension);
		
		throwIfReadOnly("Creating a new node");
		throwIfNameIsNotUnique(String.format("Node with name %s already exists", node.getName()), 
				partitionDimension.getNodes(),
				node);
		
		NodeDao nodeDao = new NodeDao(dataSourceProvider.getDataSource(this.getHiveUri()));
		nodeDao.create(node);

		incrementAndPersistHive(dataSourceProvider.getDataSource(this.getHiveUri()));
		sync();
		return node;
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
		PartitionDimension partitionDimension = getPartitionDimension(dimensionName);
		resource.setPartitionDimension(partitionDimension);
		throwIfReadOnly("Creating a new resource");
		throwIfNameIsNotUnique(String.format(
				"Resource %s already exists in the partition dimension %s",
				resource.getName(), partitionDimension.getName()),
				partitionDimension.getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.create(resource);
		incrementAndPersistHive(hiveDataSource);

		sync();
		return this.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName());
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

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
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

		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
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
				node.getName()), this.getPartitionDimension(node.getPartitionDimensionId()).getNodes(), node);

		new NodeDao(hiveDataSource).update(node);
		incrementAndPersistHive(hiveDataSource);
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

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.update(resource);
		incrementAndPersistHive(hiveDataSource);

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

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.update(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);

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
		
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.delete(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
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
								node.getName(), node.getPartitionDimension().getName()),
				node.getPartitionDimension().getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.delete(node);
		incrementAndPersistHive(hiveDataSource);
		sync();
		
		//Synchronize the DataSourceCache
		this.jdbcDaoSupportCaches.get(node.getPartitionDimension().getName()).sync();
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
		
		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.delete(resource);
		incrementAndPersistHive(hiveDataSource);
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
		
		SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryindexDao.delete(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
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
		Node node = getPartitionDimension(partitionDimensionName).getAssigner().chooseNode(
				getPartitionDimension(partitionDimensionName).getNodes(), primaryIndexKey);
		throwIfReadOnly("Inserting a new primary index key", node);
		directories.get(partitionDimensionName).insertPrimaryIndexKey(node,
				primaryIndexKey);
	}

	/***
	 * Inserts a new resource id.
	 * @param partitionDimensionName
	 * @param resourceName
	 * @param id
	 * @param primaryIndexKey
	 */
	public void insertResourceId(String partitionDimensionName, String resourceName, Object id, Object primaryIndexKey) throws HiveReadOnlyException{
		PartitionDimension dimension = getPartitionDimension(partitionDimensionName);
		Resource resource = dimension.getResource(resourceName);
		throwIfReadOnly("Inserting a new resource id");
		directories.get(dimension.getName()).insertResourceId(resource, id, primaryIndexKey);
		partitionStatistics.incrementChildRecordCount(dimension, primaryIndexKey, 1);
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
	private void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		boolean primaryKeyReadOnly = getReadOnOfResourceId(partitionDimensionName, secondaryIndex.getResource().getName(), resourceId);
		for(Integer id : directories.get(partitionDimensionName).getNodeIdsOfResourceId(secondaryIndex.getResource(), resourceId))
			throwIfReadOnly("Inserting a new secondary index key", getPartitionDimension(partitionDimensionName).getNode(id), resourceId, primaryKeyReadOnly);

		throwIfReadOnly("Inserting a new secondary index key");
		directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						resourceId);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
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
	public void insertSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				resourceId);
	}
	
	public void insertRelatedSecondaryIndexKeys(String partitionDimensionName, String resourceName, Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException {
		boolean readOnly = getReadOnOfResourceId(partitionDimensionName, resourceName, resourceId);
		for(Integer id : 
			directories.get(partitionDimensionName)
			.getNodeIdsOfResourceId(getPartitionDimension(partitionDimensionName).getResource(resourceName), resourceId))
			throwIfReadOnly("Inserting a new resource id", getPartitionDimension(partitionDimensionName).getNode(id), id, readOnly);
		
		Integer indexesUpdated = directories.get(partitionDimensionName).batch().insertSecondaryIndexKeys(secondaryIndexValueMap, resourceId);

		partitionStatistics.incrementChildRecordCount(
				getPartitionDimension(partitionDimensionName).getResource(resourceName), 
				resourceId, 
				indexesUpdated);
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
		getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey);
		directories.get(partitionDimension.getName()).updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
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
	private void updateResourceIdOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey,
			Object originalResourceId, Object newResourceId) throws HiveReadOnlyException {
		throwIfReadOnly("Updating resource id of secondary index key");
		if(directories.get(secondaryIndex.getResource().getPartitionDimension().getName()).getReadOnlyOfResourceId(secondaryIndex.getResource(), newResourceId))
			throw new HiveReadOnlyException("Updating resource id of secondary index key");
		directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
				.updateResourceIdOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey, originalResourceId, newResourceId);

		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), originalResourceId, 1);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), newResourceId, 1);
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
	public void updateResourceIdOfSecondaryIndexKey(
			String secondaryIndexName, String resourceName,
			String partitionDimensionName, Object secondaryIndexKey,
			Object originalResourceId,  Object newResourceId) throws HiveReadOnlyException {
		updateResourceIdOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				originalResourceId, newResourceId);
	}

	/***
	 * 
	 * @param resourceName
	 * @param partitionDimensionName
	 * @param resourceId
	 * @param originalPrimaryIndexKey
	 * @param newPrimaryIdnexKey
	 * @throws HiveReadOnlyException 
	 */
	public void updatePrimaryIndexKeyOfResourceId(String partitionDimensionName,String resourceName, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		throwIfReadOnly("Updating primary index key of resource id");
		if(directories.get(partitionDimensionName).getReadOnlyOfPrimaryIndexKey(newPrimaryIndexKey))
			throw new HiveReadOnlyException("Updating primary index key of resource id");
		directories.get(partitionDimensionName).updatePrimaryIndexKeyOfResourceId(
				getPartitionDimension(partitionDimensionName).getResource(resourceName), 
				resourceId, 
				originalPrimaryIndexKey, 
				newPrimaryIndexKey);
	}
	
	/**
	 * Deletes the primary index key of the given partition dimension
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 */
	private void deletePrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveReadOnlyException {

		if (!doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey))
			throw new HiveKeyNotFoundException("The primary index key " + primaryIndexKey
					+ " does not exist",primaryIndexKey);
		
		for(NodeSemaphore node: getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey)){
			throwIfReadOnly("Deleting primary index key", partitionDimension.getNode(node.getId()), primaryIndexKey,
					getReadOnlyOfPrimaryIndexKey(partitionDimension.getName(),
							primaryIndexKey));
		}
		
		Directory directory = directories.get(partitionDimension.getName());
		
		for (Resource resource : partitionDimension.getResources()){
			for(Object resourceId : directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey))
				directory.deleteResourceId(resource, resourceId);
		}
		directory.deletePrimaryIndexKey(primaryIndexKey);
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

	/***
	 * 
	 * @param resourceName
	 * @param partitionDimensionName
	 * @param id
	 * @throws HiveReadOnlyException 
	 */
	public void deleteResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		deleteResourceId(partitionDimensions.get(partitionDimensionName).getResource(resourceName), id);
	}
	
	private void deleteResourceId(final Resource resource, Object id) throws HiveReadOnlyException {
		throwIfReadOnly("Deleting resource id");
		Directory directory = directories.get(resource.getPartitionDimension().getName());
		Collection<Integer> semaphores = directory.getNodeIdsOfResourceId(resource, id);
		throwIfReadOnly("Deleting resource id", Transform.map(new Unary<Integer, Node>(){
			public Node f(Integer item) {
				return resource.getPartitionDimension().getNode(item);
			}}, semaphores));
		// TODO check primary index key readOnly
		deleteAllSecondaryIndexKeysOfResourceId(resource.getPartitionDimension().getName(), resource.getName(), id);
		partitionStatistics.decrementChildRecordCount(
				resource.getPartitionDimension(), 
				directories.get(resource.getPartitionDimension().getName()).getPrimaryIndexKeyOfResourceId(resource, id),1);
		directory.deleteResourceId(resource, id);
	}
	
	public void deleteAllSecondaryIndexKeysOfResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		throwIfReadOnly("Deleting resource id");
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		final Resource resource = partitionDimension.getResource(resourceName);
		Directory directory = directories.get(partitionDimensionName);
		Collection<Integer> semaphores = directory.getNodeIdsOfResourceId(resource, id);
		throwIfReadOnly("Deleting resource id", Transform.map(new Unary<Integer, Node>(){
			public Node f(Integer item) {
				return resource.getPartitionDimension().getNode(item);
			}}, semaphores));
		// TODO check primary index key readOnly
		Integer rowsAffected = directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
		partitionStatistics.decrementChildRecordCount(resource, id, rowsAffected);
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
	private void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException{
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		boolean primaryKeyReadOnly = getReadOnlyOfPrimaryIndexKey(partitionDimensionName, resourceId);
		for(Integer id : directories.get(partitionDimensionName).getNodeIdsOfPrimaryIndexKey(resourceId))
			throwIfReadOnly("Deleting secondary index key", getPartitionDimension(partitionDimensionName).getNode(id), resourceId, primaryKeyReadOnly);
		if (!doesSecondaryIndexKeyExist(secondaryIndex.getName(), secondaryIndex.getResource().getName(), partitionDimensionName, secondaryIndexKey))
			throw new HiveKeyNotFoundException(
					String.format("Secondary index key %s of secondary index %s does not exist",secondaryIndexKey,secondaryIndex.getName()),secondaryIndexKey);

		directories.get(partitionDimensionName)
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
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
	public void deleteSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		deleteSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey, resourceId);
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
	public boolean doesPrimaryIndexKeyExist(String partitionDimensionName,
			Object primaryIndexKey) {
		return directories.get(partitionDimensionName).doesPrimaryIndexKeyExist(
				primaryIndexKey);
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
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @return
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) {
		Boolean readOnly = directories.get(partitionDimensionName)
			.getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
		if (readOnly != null)
			return readOnly;
		throw new HiveKeyNotFoundException(String.format(
				"Primary index key %s of partition dimension %s not found.",
				primaryIndexKey.toString(), partitionDimensionName),primaryIndexKey);
	}
	
	public boolean getReadOnOfResourceId(String partitionDimensionName, String resourceName, Object id) {
		return directories.get(partitionDimensionName)
			.getReadOnlyOfResourceId(getPartitionDimension(partitionDimensionName)
					.getResource(resourceName), id);
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
	public boolean doesSecondaryIndexKeyExist(String secondaryIndexName, String resourceName, String dimensionName,
			Object secondaryIndexKey) {
		SecondaryIndex secondaryIndex = getPartitionDimension(dimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexName);
		return directories.get(
				secondaryIndex.getResource().getPartitionDimension().getName())
				.doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey);
	}

	public boolean doesResourceIdExist(String resourceName, String partitionDimensionName, Object id) {
		return directories.get(partitionDimensionName).doesResourceIdExist(
				partitionDimensions.get(partitionDimensionName).getResource(resourceName), id);
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
								secondaryIndexKey.toString(), partitionDimension
										.getName(), secondaryIndex.getName()), primaryIndexKeys);
	}

	@SuppressWarnings("unchecked")
	public Collection<Object> getResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		return directories.get(secondaryIndex.getResource().getPartitionDimension().getName()).getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
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
	private Collection getSecondaryIndexKeysWithPrimaryKey(
			SecondaryIndex secondaryIndex, Object primaryIndexKey) {
		return directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
					.getSecondaryIndexKeysOfPrimaryIndexKey(secondaryIndex, primaryIndexKey);
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
			String secondaryIndexName,
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		return getSecondaryIndexKeysWithPrimaryKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), primaryIndexKey);
	}
	
	public Collection getResourceIdsWithPrimaryKey(
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		return directories.get(partitionDimensionName).getResourceIdsOfPrimaryIndexKey(partitionDimensions.get(partitionDimensionName).getResource(resourceName), primaryIndexKey);
	}
	
	private Connection getConnection(PartitionDimension partitionDimension, NodeSemaphore semaphore, AccessType intention) throws HiveReadOnlyException,SQLException {
		try{
			if( intention == AccessType.ReadWrite && isKeyReadOnly(partitionDimension, semaphore))
				throw new HiveReadOnlyException("The key/node/hive requested cannot be written to at this time.");
			
			Connection conn = 
				nodeDataSources.get(partitionDimension.getNode(semaphore.getId()).getUri()).getConnection();
			
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
		return isReadOnly() || partitionDimension.getNode(semaphore.getId()).isReadOnly() || semaphore.isReadOnly();
	}
	
	/***
	 * Get a JDBC connection to a data node by partition key.
	 * @param partitionDimension
	 * @param primaryIndexKey
	 * @param intent
	 * @return
	 * @throws HiveReadOnlyException 
	 */
	public Collection<Connection> getConnection(String partitionDimensionName,
			Object primaryIndexKey, AccessType intent) throws SQLException, HiveReadOnlyException {
		Collection<Connection> connections = new ArrayList<Connection>();
		for(NodeSemaphore semaphore : getNodeSemaphoresOfPrimaryIndexKey(getPartitionDimension(partitionDimensionName), primaryIndexKey))
			connections.add(getConnection(getPartitionDimension(partitionDimensionName), semaphore, intent));
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
	public Collection<Connection> getConnection(String secondaryIndexName, String resourceName, String dimensionName,
			Object secondaryIndexKey, AccessType intent) throws HiveReadOnlyException, SQLException {
		if(AccessType.ReadWrite == intent)
			throw new UnsupportedOperationException("Writes must be performed using the primary index key.");
		
		SecondaryIndex secondaryIndex = getPartitionDimension(dimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexName);
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
	 * 
	 * @param resourceName
	 * @param dimensionName
	 * @param resourceId
	 * @param intent
	 * @return
	 * @throws SQLException 
	 * @throws HiveReadOnlyException 
	 */
	public Collection<Connection> getConnection(String resourceName, String dimensionName, Object resourceId, AccessType intent) throws HiveReadOnlyException, SQLException {
		Collection<Connection> connections = new ArrayList<Connection>();
		Directory directory = directories.get(dimensionName);
		Resource resource = partitionDimensions.get(dimensionName).getResource(resourceName);
		for(NodeSemaphore semaphore : directory.getNodeSemaphoresOfResourceId(resource, resourceId))
			connections.add(getConnection(getPartitionDimension(dimensionName), semaphore, intent));
		return connections;
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

	public HiveDbDialect getDialect() {
		return DriverLoader.discernDialect(hiveUri);
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
		notifyObservers();
	}
	
	public void notifyObservers() {
		super.setChanged();
		super.notifyObservers();
	}
}
