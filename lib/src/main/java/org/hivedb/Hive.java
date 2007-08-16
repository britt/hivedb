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
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.IdAndNameIdentifiable;
import org.hivedb.meta.Identifiable;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeSemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.directory.Directory;
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
import org.hivedb.util.Preconditions;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Unary;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class Hive extends Observable implements Synchronizeable, Observer, Lockable {

	public static final int NEW_OBJECT_ID = 0;
	//logger
	private static Logger log = Logger.getLogger(Hive.class);
	//constants
	private static final int DEFAULT_JDBC_TIMEOUT = 500;
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


	/* (non-Javadoc)
	 * @see org.hivedb.IHive#sync()
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getHiveUri()
	 */
	public String getUri() {
		return hiveUri;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#hashCode()
	 */
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, revision, getPartitionDimensions(), readOnly });
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#isReadOnly()
	 */
	/* (non-Javadoc)
	 * @see org.hivedb.Lockable#isReadOnly()
	 */
	public boolean isReadOnly() {
		return readOnly;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateHiveReadOnly(java.lang.Boolean)
	 */
	public void updateHiveReadOnly(Boolean readOnly) {
		this.readOnly = readOnly;
		new HiveSemaphoreDao(hiveDataSource).update(new HiveSemaphore(
				readOnly, this.getRevision()));
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateNodeReadOnly(org.hivedb.meta.Node, java.lang.Boolean)
	 */
	public void updateNodeReadOnly(Node node, Boolean readOnly){
		node.setReadOnly(readOnly);
		try {
			this.updateNode(node);
		} catch (HiveReadOnlyException e) {
			//quash since the hive is already read-only
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getRevision()
	 */
	public int getRevision() {
		return revision;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPartitionDimensions()
	 */
	public Collection<PartitionDimension> getPartitionDimensions() {
		return partitionDimensions.values();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPartitionDimension(java.lang.String)
	 */
	public PartitionDimension getPartitionDimension(String name) {
		if (!partitionDimensions.containsKey(name))
			throw new HiveRuntimeException(String.format("Unknown partition dimension %s", name));
		return partitionDimensions.get(name);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPartitionDimension(int)
	 */
	public PartitionDimension getPartitionDimension(final int id) {
		return Filter.grepSingle(new Predicate<PartitionDimension>(){

			public boolean f(PartitionDimension item) {
				return item.getId() == id;
			}}, partitionDimensions.values());
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#addPartitionDimension(org.hivedb.meta.PartitionDimension)
	 */
	public PartitionDimension addPartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(getPartitionDimensions(), partitionDimension);
		
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#addNode(org.hivedb.meta.PartitionDimension, org.hivedb.meta.Node)
	 */
	public Node addNode(PartitionDimension partitionDimension, Node node)
			throws HiveReadOnlyException {
		
		node.setPartitionDimension(partitionDimension);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(partitionDimension.getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(dataSourceProvider.getDataSource(this.getUri()));
		nodeDao.create(node);

		incrementAndPersistHive(dataSourceProvider.getDataSource(this.getUri()));
		sync();
		return node;
	}

	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#addResource(java.lang.String, org.hivedb.meta.Resource)
	 */
	public Resource addResource(String dimensionName,
			Resource resource) throws HiveReadOnlyException{
		PartitionDimension partitionDimension = getPartitionDimension(dimensionName);
		resource.setPartitionDimension(partitionDimension);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(partitionDimension.getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.create(resource);
		incrementAndPersistHive(hiveDataSource);

		sync();
		return this.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName());
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#addSecondaryIndex(org.hivedb.meta.Resource, org.hivedb.meta.SecondaryIndex)
	 */
	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		secondaryIndex.setResource(resource);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(resource.getSecondaryIndexes(), secondaryIndex);
		
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		sync();

		new IndexSchema(getPartitionDimension(resource.getPartitionDimension().getName())).install();
		return secondaryIndex;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updatePartitionDimension(org.hivedb.meta.PartitionDimension)
	 */
	public PartitionDimension updatePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException  {
		
		Preconditions.isWritable(this);
		throwIfIdNotPresent(String.format(
				"Partition dimension with id %s does not exist",
				partitionDimension.getId()), getPartitionDimensions(),
				partitionDimension);

		Preconditions.nameIsUnique(getPartitionDimensions(), partitionDimension);

		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
		sync();
		
		return partitionDimension;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateNode(org.hivedb.meta.Node)
	 */
	public Node updateNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		throwIfIdNotPresent(String.format("Node with id %s does not exist",
				node.getName()), this.getPartitionDimension(node.getPartitionDimensionId()).getNodes(), node);

		new NodeDao(hiveDataSource).update(node);
		incrementAndPersistHive(hiveDataSource);
		sync();
		return node;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateResource(org.hivedb.meta.Resource)
	 */
	public Resource updateResource(Resource resource) throws HiveReadOnlyException  {
		Preconditions.isWritable(this);
		throwIfIdNotPresent(String.format(
				"Resource with id %s does not exist", resource.getId()),
				resource.getPartitionDimension().getResources(), resource);
		Preconditions.nameIsUnique(resource.getPartitionDimension().getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.update(resource);
		incrementAndPersistHive(hiveDataSource);

		sync();
		return resource;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateSecondaryIndex(org.hivedb.meta.SecondaryIndex)
	 */
	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException  {
		Preconditions.isWritable(this);
		throwIfIdNotPresent(String.format(
				"Secondary index with id %s does not exist", secondaryIndex
						.getId()), secondaryIndex.getResource()
				.getSecondaryIndexes(), secondaryIndex);
		Preconditions.nameIsUnique(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.update(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);

		sync();
		return secondaryIndex;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deletePartitionDimension(org.hivedb.meta.PartitionDimension)
	 */
	public PartitionDimension deletePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteNode(org.hivedb.meta.Node)
	 */
	public Node deleteNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteResource(org.hivedb.meta.Resource)
	 */
	public Resource deleteResource(Resource resource) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteSecondaryIndex(org.hivedb.meta.SecondaryIndex)
	 */
	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		Preconditions.isWritable(this);
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#insertPrimaryIndexKey(java.lang.String, java.lang.Object)
	 */
	public void insertPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveReadOnlyException {
		Node node = getPartitionDimension(partitionDimensionName).getAssigner().chooseNode(
				getPartitionDimension(partitionDimensionName).getNodes(), primaryIndexKey);
		Preconditions.isWritable(this, node);
		directories.get(partitionDimensionName).insertPrimaryIndexKey(node,
				primaryIndexKey);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#insertResourceId(java.lang.String, java.lang.String, java.lang.Object, java.lang.Object)
	 */
	public void insertResourceId(String partitionDimensionName, String resourceName, Object id, Object primaryIndexKey) throws HiveReadOnlyException{
		PartitionDimension dimension = getPartitionDimension(partitionDimensionName);
		Resource resource = dimension.getResource(resourceName);
		Preconditions.isWritable(this);
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey));
		
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
		
		Preconditions.isWritable(this);
		Preconditions.isWritable(
				directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), resourceId));
		
		directories.get(secondaryIndex.getResource().getPartitionDimension().getName())
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						resourceId);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#insertSecondaryIndexKey(java.lang.String, java.lang.String, java.lang.String, java.lang.Object, java.lang.Object)
	 */
	public void insertSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				resourceId);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#insertRelatedSecondaryIndexKeys(java.lang.String, java.lang.String, java.util.Map, java.lang.Object)
	 */
	public void insertRelatedSecondaryIndexKeys(String partitionDimensionName, String resourceName, Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(getPartitionDimension(partitionDimensionName).getResource(resourceName), resourceId));
		
		Integer indexesUpdated = directories.get(partitionDimensionName).batch().insertSecondaryIndexKeys(secondaryIndexValueMap, resourceId);
		partitionStatistics.incrementChildRecordCount(
				getPartitionDimension(partitionDimensionName).getResource(resourceName), 
				resourceId, 
				indexesUpdated);
	}
		
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updatePrimaryIndexReadOnly(java.lang.String, java.lang.Object, boolean)
	 */
	public void updatePrimaryIndexReadOnly(String partitionDimensionName,
			Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		Preconditions.isWritable(this);
		Preconditions.isWritable(getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey));
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
		Directory directory = directories.get(secondaryIndex.getResource().getPartitionDimension().getName());
		Preconditions.isWritable(this);
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), newResourceId));
		
		directory.updateResourceIdOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey, originalResourceId, newResourceId);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), originalResourceId, 1);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), newResourceId, 1);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updateResourceIdOfSecondaryIndexKey(java.lang.String, java.lang.String, java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object)
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#updatePrimaryIndexKeyOfResourceId(java.lang.String, java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	public void updatePrimaryIndexKeyOfResourceId(String partitionDimensionName,String resourceName, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(newPrimaryIndexKey));
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
		
		Preconditions.isWritable(this);
		Preconditions.isWritable(getNodeSemaphoresOfPrimaryIndexKey(partitionDimension, primaryIndexKey));
		
		Directory directory = directories.get(partitionDimension.getName());
		for (Resource resource : partitionDimension.getResources()){
			for(Object resourceId : directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey))
				directory.deleteResourceId(resource, resourceId);
		}
		directory.deletePrimaryIndexKey(primaryIndexKey);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deletePrimaryIndexKey(java.lang.String, java.lang.Object)
	 */
	public void deletePrimaryIndexKey(String partitionDimensionName,
			Object secondaryIndexKey) throws HiveReadOnlyException{
		deletePrimaryIndexKey(getPartitionDimension(partitionDimensionName),
				secondaryIndexKey);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteResourceId(java.lang.String, java.lang.String, java.lang.Object)
	 */
	public void deleteResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		deleteResourceId(partitionDimensions.get(partitionDimensionName).getResource(resourceName), id);
	}
	
	private void deleteResourceId(final Resource resource, Object id) throws HiveReadOnlyException {
		
		Directory directory = directories.get(resource.getPartitionDimension().getName());
		Preconditions.isWritable(this);
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(resource, id));
	
		deleteAllSecondaryIndexKeysOfResourceId(resource.getPartitionDimension().getName(), resource.getName(), id);
		partitionStatistics.decrementChildRecordCount(
				resource.getPartitionDimension(), 
				directories.get(resource.getPartitionDimension().getName()).getPrimaryIndexKeyOfResourceId(resource, id),1);
		directory.deleteResourceId(resource, id);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteAllSecondaryIndexKeysOfResourceId(java.lang.String, java.lang.String, java.lang.Object)
	 */
	public void deleteAllSecondaryIndexKeysOfResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		final Resource resource = partitionDimension.getResource(resourceName);
		Directory directory = directories.get(partitionDimensionName);
		
		Preconditions.isWritable(this);
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(resource, id));
		
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
		
		Preconditions.isWritable(this);
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), resourceId));
		
		if (!doesSecondaryIndexKeyExist(secondaryIndex.getName(), secondaryIndex.getResource().getName(), partitionDimensionName, secondaryIndexKey))
			throw new HiveKeyNotFoundException(
					String.format("Secondary index key %s of secondary index %s does not exist",secondaryIndexKey,secondaryIndex.getName()),secondaryIndexKey);

		directories.get(partitionDimensionName)
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#deleteSecondaryIndexKey(java.lang.String, java.lang.String, java.lang.String, java.lang.Object, java.lang.Object)
	 */
	public void deleteSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		deleteSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey, resourceId);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#doesPrimaryIndexKeyExist(java.lang.String, java.lang.Object)
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getReadOnlyOfPrimaryIndexKey(java.lang.String, java.lang.Object)
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
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getReadOnOfResourceId(java.lang.String, java.lang.String, java.lang.Object)
	 */
	public boolean getReadOnlyOfResourceId(String partitionDimensionName, String resourceName, Object id) {
		return directories.get(partitionDimensionName)
			.getReadOnlyOfResourceId(getPartitionDimension(partitionDimensionName)
					.getResource(resourceName), id);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#doesSecondaryIndexKeyExist(java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
	 */
	public boolean doesSecondaryIndexKeyExist(String secondaryIndexName, String resourceName, String dimensionName,
			Object secondaryIndexKey) {
		SecondaryIndex secondaryIndex = getPartitionDimension(dimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexName);
		return directories.get(
				secondaryIndex.getResource().getPartitionDimension().getName())
				.doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#doesResourceIdExist(java.lang.String, java.lang.String, java.lang.Object)
	 */
	public boolean doesResourceIdExist(String resourceName, String partitionDimensionName, Object id) {
		return directories.get(partitionDimensionName).doesResourceIdExist(
				partitionDimensions.get(partitionDimensionName).getResource(resourceName), id);
	}
	
	private Collection<NodeSemaphore> getNodeSemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) {
		return directories.get(secondaryIndex.getResource().getPartitionDimension().getName()).getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPrimaryIndexKeysOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getResourceIdsOfSecondaryIndexKey(org.hivedb.meta.SecondaryIndex, java.lang.Object)
	 */
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getSecondaryIndexKeysWithPrimaryKey(java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
	 */
	public Collection getSecondaryIndexKeysWithPrimaryKey(
			String secondaryIndexName,
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		return getSecondaryIndexKeysWithPrimaryKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), primaryIndexKey);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getResourceIdsWithPrimaryKey(java.lang.String, java.lang.String, java.lang.Object)
	 */
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
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getConnection(java.lang.String, java.lang.Object, org.hivedb.meta.AccessType)
	 */
	public Collection<Connection> getConnection(String partitionDimensionName,
			Object primaryIndexKey, AccessType intent) throws SQLException, HiveReadOnlyException {
		Collection<Connection> connections = new ArrayList<Connection>();
		for(NodeSemaphore semaphore : getNodeSemaphoresOfPrimaryIndexKey(getPartitionDimension(partitionDimensionName), primaryIndexKey))
			connections.add(getConnection(getPartitionDimension(partitionDimensionName), semaphore, intent));
		return connections;
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getConnection(java.lang.String, java.lang.String, java.lang.String, java.lang.Object, org.hivedb.meta.AccessType)
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
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getConnection(java.lang.String, java.lang.String, java.lang.Object, org.hivedb.meta.AccessType)
	 */
	public Collection<Connection> getConnection(String resourceName, String dimensionName, Object resourceId, AccessType intent) throws HiveReadOnlyException, SQLException {
		Collection<Connection> connections = new ArrayList<Connection>();
		Directory directory = directories.get(dimensionName);
		Resource resource = partitionDimensions.get(dimensionName).getResource(resourceName);
		for(NodeSemaphore semaphore : directory.getNodeSemaphoresOfResourceId(resource, resourceId))
			connections.add(getConnection(getPartitionDimension(dimensionName), semaphore, intent));
		return connections;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getJdbcDaoSupportCache(java.lang.String)
	 */
	public JdbcDaoSupportCache getJdbcDaoSupportCache(String partitionDimensionName) {
		return this.jdbcDaoSupportCaches.get(partitionDimensionName);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#toString()
	 */
	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimensions());
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

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getDialect()
	 */
	public HiveDbDialect getDialect() {
		return DriverLoader.discernDialect(hiveUri);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPartitionStatistics()
	 */
	public PartitionKeyStatisticsDao getPartitionStatistics() {
		return partitionStatistics;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#getPerformanceStatistics()
	 */
	public HivePerformanceStatistics getPerformanceStatistics() {
		return performanceStatistics;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#setPerformanceStatistics(org.hivedb.management.statistics.HivePerformanceStatistics)
	 */
	public void setPerformanceStatistics(HivePerformanceStatistics performanceStatistics) {
		this.performanceStatistics = performanceStatistics;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#isPerformanceMonitoringEnabled()
	 */
	public boolean isPerformanceMonitoringEnabled() {
		return performanceStatistics != null && performanceMonitoringEnabled;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#setPerformanceMonitoringEnabled(boolean)
	 */
	public void setPerformanceMonitoringEnabled(boolean performanceMonitoringEnabled) {
		this.performanceMonitoringEnabled = performanceMonitoringEnabled;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.IHive#update(java.util.Observable, java.lang.Object)
	 */
	public void update(Observable o, Object arg) {
		sync();
		notifyObservers();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.IHive#notifyObservers()
	 */
	public void notifyObservers() {
		super.setChanged();
		super.notifyObservers();
	}
}
