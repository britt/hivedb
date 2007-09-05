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
import org.hivedb.management.statistics.HivePerformanceStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.KeySemaphore;
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
		//Inject the statistics monitoring beans
		if( hiveStats != null) {
			hive.setPerformanceStatistics(hiveStats);
			hive.setPerformanceMonitoringEnabled(true);
		} 
		hive.sync();
		log.debug("Successfully loaded Hive from " + hiveDatabaseUri);
		return hive;
	}


	public boolean sync() {
		boolean updated = false;
		HiveSemaphore hs = new HiveSemaphoreDao(hiveDataSource).get();
		
		if(this.revision != hs.getRevision()) {
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
			updated = true;
		}
		return updated;
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

	public String getUri() {
		return hiveUri;
	}

	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, revision, getPartitionDimensions(), readOnly });
	}

	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void updateHiveReadOnly(Boolean readOnly) {
		this.readOnly = readOnly;
		new HiveSemaphoreDao(hiveDataSource).update(new HiveSemaphore(
				readOnly, this.getRevision()));
	}

	public void updateNodeReadOnly(Node node, Boolean readOnly){
		node.setReadOnly(readOnly);
		try {
			this.updateNode(node);
		} catch (HiveReadOnlyException e) {
			//quash since the hive is already read-only
		}
	}
	
	public int getRevision() {
		return revision;
	}

	public Collection<PartitionDimension> getPartitionDimensions() {
		return partitionDimensions.values();
	}
	
	public PartitionDimension getPartitionDimension(String name) {
		if (!partitionDimensions.containsKey(name))
			throw new HiveKeyNotFoundException(String.format("Unknown partition dimension %s", name), name);
		return partitionDimensions.get(name);
	}
	
	public PartitionDimension getPartitionDimension(final int id) {
		return Filter.grepSingle(new Predicate<PartitionDimension>(){

			public boolean f(PartitionDimension item) {
				return item.getId() == id;
			}}, partitionDimensions.values());
	}
	
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

	public Node addNode(PartitionDimension partitionDimension, Node node)
			throws HiveReadOnlyException {
		
		node.setPartitionDimension(partitionDimension);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(partitionDimension.getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(dataSourceProvider.getDataSource(this.getUri()));
		nodeDao.create(node);

		incrementAndPersistHive(dataSourceProvider.getDataSource(this.getUri()));
		return node;
	}

	public Resource addResource(String dimensionName,
			Resource resource) throws HiveReadOnlyException{
		PartitionDimension partitionDimension = getPartitionDimension(dimensionName);
		resource.setPartitionDimension(partitionDimension);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(partitionDimension.getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.create(resource);
		incrementAndPersistHive(hiveDataSource);
		return this.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName());
	}

	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		secondaryIndex.setResource(resource);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(resource.getSecondaryIndexes(), secondaryIndex);
		
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		new IndexSchema(getPartitionDimension(resource.getPartitionDimension().getName())).install();
		return secondaryIndex;
	}

	public PartitionDimension updatePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException  {
		
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getPartitionDimensions(), partitionDimension);
		Preconditions.nameIsUnique(getPartitionDimensions(), partitionDimension);

		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
		
		return partitionDimension;
	}

	public Node updateNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getPartitionDimension(node.getPartitionDimensionId()).getNodes(), node);

		new NodeDao(hiveDataSource).update(node);
		incrementAndPersistHive(hiveDataSource);
		return node;
	}

	public Resource updateResource(Resource resource) throws HiveReadOnlyException  {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(resource.getPartitionDimension().getResources(), resource);
		Preconditions.nameIsUnique(resource.getPartitionDimension().getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.update(resource);
		incrementAndPersistHive(hiveDataSource);

		return resource;
	}

	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException  {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);
		Preconditions.nameIsUnique(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.update(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);

		return secondaryIndex;
	}

	public PartitionDimension deletePartitionDimension(
			PartitionDimension partitionDimension) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getPartitionDimensions(), partitionDimension);
		
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.delete(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
		
		//Destroy the corresponding DataSourceCache
		this.jdbcDaoSupportCaches.remove(partitionDimension.getName());
		
		return partitionDimension;
	}

	public Node deleteNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(node.getPartitionDimension().getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.delete(node);
		incrementAndPersistHive(hiveDataSource);
		//Synchronize the DataSourceCache
		this.jdbcDaoSupportCaches.get(node.getPartitionDimension().getName()).sync();
		return node;
	}

	public Resource deleteResource(Resource resource) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(resource.getPartitionDimension().getResources(), resource);
		
		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.delete(resource);
		incrementAndPersistHive(hiveDataSource);
		
		return resource;
	}

	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryindexDao.delete(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		
		return secondaryIndex;
	}

	private void incrementAndPersistHive(DataSource datasource) {
		new HiveSemaphoreDao(datasource).incrementAndPersist();
		this.sync();
	}

	public void insertPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveReadOnlyException {
		Node node = getPartitionDimension(partitionDimensionName).getAssigner().chooseNode(
				getPartitionDimension(partitionDimensionName).getNodes(), primaryIndexKey);
		Preconditions.isWritable(this, node);
		directories.get(partitionDimensionName).insertPrimaryIndexKey(node,
				primaryIndexKey);
	}

	public void insertResourceId(String partitionDimensionName, String resourceName, Object id, Object primaryIndexKey) throws HiveReadOnlyException{
		PartitionDimension dimension = getPartitionDimension(partitionDimensionName);
		Resource resource = dimension.getResource(resourceName);
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(semaphores, this);
		directories.get(dimension.getName()).insertResourceId(resource, id, primaryIndexKey);
		partitionStatistics.incrementChildRecordCount(dimension, primaryIndexKey, 1);
	}

	private void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		Collection<KeySemaphore> semaphores = 
			directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), resourceId);
		Preconditions.isWritable(semaphores, this);
		directories.get(partitionDimensionName)
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						resourceId);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
	}

	public void insertSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				resourceId);
	}
	
	public void insertRelatedSecondaryIndexKeys(String partitionDimensionName, String resourceName, Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException {
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(getPartitionDimension(partitionDimensionName).getResource(resourceName), resourceId);
		Preconditions.isWritable(semaphores,this);
		Integer indexesUpdated = directories.get(partitionDimensionName).batch().insertSecondaryIndexKeys(secondaryIndexValueMap, resourceId);
		partitionStatistics.incrementChildRecordCount(
				getPartitionDimension(partitionDimensionName).getResource(resourceName), 
				resourceId, 
				indexesUpdated);
	}

	public void updatePrimaryIndexReadOnly(String partitionDimensionName,
			Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(semaphores);
		
		directories.get(partitionDimension.getName()).updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
	}

	private void updateResourceIdOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey,
			Object originalResourceId, Object newResourceId) throws HiveReadOnlyException {
		Directory directory = directories.get(secondaryIndex.getResource().getPartitionDimension().getName());
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), newResourceId),this);
		
		directory.updateResourceIdOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey, originalResourceId, newResourceId);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), originalResourceId, 1);
		partitionStatistics.incrementChildRecordCount(secondaryIndex.getResource(), newResourceId, 1);
	}

	public void updateResourceIdOfSecondaryIndexKey(
			String secondaryIndexName, String resourceName,
			String partitionDimensionName, Object secondaryIndexKey,
			Object originalResourceId,  Object newResourceId) throws HiveReadOnlyException {
		updateResourceIdOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				originalResourceId, newResourceId);
	}

	public void updatePrimaryIndexKeyOfResourceId(String partitionDimensionName,String resourceName, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(newPrimaryIndexKey), this);
		directories.get(partitionDimensionName).updatePrimaryIndexKeyOfResourceId(
				getPartitionDimension(partitionDimensionName).getResource(resourceName), 
				resourceId, 
				originalPrimaryIndexKey, 
				newPrimaryIndexKey);
	}
	
	private void deletePrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveReadOnlyException {

		if (!doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey))
			throw new HiveKeyNotFoundException("The primary index key " + primaryIndexKey
					+ " does not exist",primaryIndexKey);
		
		Preconditions.isWritable(directories.get(partitionDimension.getName()).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey), this);
		
		Directory directory = directories.get(partitionDimension.getName());
		for (Resource resource : partitionDimension.getResources()){
			for(Object resourceId : directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey))
				directory.deleteResourceId(resource, resourceId);
		}
		directory.deletePrimaryIndexKey(primaryIndexKey);
	}

	public void deletePrimaryIndexKey(String partitionDimensionName,
			Object secondaryIndexKey) throws HiveReadOnlyException{
		deletePrimaryIndexKey(getPartitionDimension(partitionDimensionName),
				secondaryIndexKey);
	}

	public void deleteResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		deleteResourceId(partitionDimensions.get(partitionDimensionName).getResource(resourceName), id);
	}
	
	private void deleteResourceId(final Resource resource, Object id) throws HiveReadOnlyException {
		
		Directory directory = directories.get(resource.getPartitionDimension().getName());
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(resource, id), this);
	
		deleteAllSecondaryIndexKeysOfResourceId(resource.getPartitionDimension().getName(), resource.getName(), id);
		partitionStatistics.decrementChildRecordCount(
				resource.getPartitionDimension(), 
				directories.get(resource.getPartitionDimension().getName()).getPrimaryIndexKeyOfResourceId(resource, id),1);
		directory.deleteResourceId(resource, id);
	}
	
	public void deleteAllSecondaryIndexKeysOfResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		final Resource resource = partitionDimension.getResource(resourceName);
		Directory directory = directories.get(partitionDimensionName);
		
		Preconditions.isWritable(directory.getNodeSemaphoresOfResourceId(resource, id),this);
		
		Integer rowsAffected = directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
		partitionStatistics.decrementChildRecordCount(resource, id, rowsAffected);
	}
	
	private void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException{
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		
		Preconditions.isWritable(directories.get(partitionDimensionName).getNodeSemaphoresOfResourceId(secondaryIndex.getResource(), resourceId),this);
		
		if (!doesSecondaryIndexKeyExist(secondaryIndex.getName(), secondaryIndex.getResource().getName(), partitionDimensionName, secondaryIndexKey))
			throw new HiveKeyNotFoundException(
					String.format("Secondary index key %s of secondary index %s does not exist",secondaryIndexKey,secondaryIndex.getName()),secondaryIndexKey);

		directories.get(partitionDimensionName)
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
		partitionStatistics.decrementChildRecordCount(secondaryIndex.getResource(), resourceId, 1);
	}

	public void deleteSecondaryIndexKey(String secondaryIndexName, String resourceName, String partitionDimensionName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		deleteSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey, resourceId);
	}

	public boolean doesPrimaryIndexKeyExist(String partitionDimensionName,
			Object primaryIndexKey) {
		return directories.get(partitionDimensionName).doesPrimaryIndexKeyExist(
				primaryIndexKey);
	}

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

	public boolean getReadOnlyOfResourceId(String partitionDimensionName, String resourceName, Object id) {
		return directories.get(partitionDimensionName)
			.getReadOnlyOfResourceId(getPartitionDimension(partitionDimensionName)
					.getResource(resourceName), id);
	}

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

	public Collection<?> getSecondaryIndexKeysWithPrimaryKey(
			String secondaryIndexName,
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		return directories.get(partitionDimensionName).getSecondaryIndexKeysOfPrimaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), primaryIndexKey);
	}

	public Collection<?> getResourceIdsWithPrimaryKey(
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		return directories.get(partitionDimensionName).getResourceIdsOfPrimaryIndexKey(partitionDimensions.get(partitionDimensionName).getResource(resourceName), primaryIndexKey);
	}
	
	private Connection getConnection(PartitionDimension partitionDimension, KeySemaphore semaphore, AccessType intention) throws HiveReadOnlyException,SQLException {
		try{
			if(intention == AccessType.ReadWrite)
				Preconditions.isWritable(this, semaphore, partitionDimension.getNode(semaphore.getId()));
			
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
	
	public Collection<Connection> getConnection(String partitionDimensionName,
			Object primaryIndexKey, AccessType intent) throws SQLException, HiveReadOnlyException {
		Collection<Connection> connections = new ArrayList<Connection>();
		for(KeySemaphore semaphore : directories.get(partitionDimensionName).getNodeSemamphoresOfPrimaryIndexKey(primaryIndexKey))
			connections.add(getConnection(getPartitionDimension(partitionDimensionName), semaphore, intent));
		return connections;
	}

	public Collection<Connection> getConnection(String secondaryIndexName, String resourceName, String dimensionName,
			Object secondaryIndexKey, AccessType intent) throws HiveReadOnlyException, SQLException {
		if(AccessType.ReadWrite == intent)
			throw new UnsupportedOperationException("Writes must be performed using the primary index key.");
		
		SecondaryIndex secondaryIndex = getPartitionDimension(dimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexName);
		Collection<Connection> connections = new ArrayList<Connection>();
		Collection<KeySemaphore> nodeSemaphores = directories.get(dimensionName).getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		nodeSemaphores = Filter.getUnique(nodeSemaphores, new Unary<KeySemaphore, Integer>(){
			public Integer f(KeySemaphore item) {
				return item.getId();
			}});
		for(KeySemaphore semaphore : nodeSemaphores)
			connections.add(getConnection(secondaryIndex.getResource().getPartitionDimension(), semaphore, intent));
		return connections;
	}
	
	public Collection<Connection> getConnection(String resourceName, String dimensionName, Object resourceId, AccessType intent) throws HiveReadOnlyException, SQLException {
		Collection<Connection> connections = new ArrayList<Connection>();
		Directory directory = directories.get(dimensionName);
		Resource resource = partitionDimensions.get(dimensionName).getResource(resourceName);
		for(KeySemaphore semaphore : directory.getNodeSemaphoresOfResourceId(resource, resourceId))
			connections.add(getConnection(getPartitionDimension(dimensionName), semaphore, intent));
		return connections;
	}

	public JdbcDaoSupportCache getJdbcDaoSupportCache(String partitionDimensionName) {
		return this.jdbcDaoSupportCaches.get(partitionDimensionName);
	}

	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimensions());
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

	/*
	 * (non-Javadoc)
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	public void update(Observable o, Object arg) {
		if(sync())
			notifyObservers();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Observable#notifyObservers()
	 */
	public void notifyObservers() {
		super.setChanged();
		super.notifyObservers();
	}
}
