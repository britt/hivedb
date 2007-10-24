/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.meta.persistence.HiveBasicDataSourceProvider;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.Preconditions;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class Hive extends Observable implements Synchronizeable, Observer, Lockable {
	//constants
	private static final int DEFAULT_JDBC_TIMEOUT = 500;
	public static final int NEW_OBJECT_ID = 0;
	
	private HiveSemaphore semaphore;
	private String hiveUri;
	
	private Map<String,PartitionDimension> partitionDimensions;
	private Map<String,ConnectionManager> connections;
	private Map<String,Directory> directories;
	private DataSource hiveDataSource;
	private DataSourceProvider dataSourceProvider;
	private Assigner defaultNodeAssigner = null;
	

	public static Hive load(String hiveDatabaseUri) {
		return load(hiveDatabaseUri, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT));
	}

	public static Hive load(String hiveDatabaseUri, DataSourceProvider dataSourceProvider) {
		return load(hiveDatabaseUri, dataSourceProvider, null);
	}

	public static Hive load(String hiveDatabaseUri, DataSourceProvider dataSourceProvider, Assigner assigner) {
		//Tickle driver
		try {
			DriverLoader.load(hiveDatabaseUri);
		} catch (ClassNotFoundException e) {
			throw new HiveRuntimeException("Unable to load database driver: " + e.getMessage(), e);
		} 
		
		Hive hive = new Hive(hiveDatabaseUri, 0, false, dataSourceProvider);
		hive.setDefaultNodeAssigner(assigner);
		
		hive.sync();
		return hive;
		
	}

	public boolean sync() {
		boolean updated = false;
		HiveSemaphore hs = new HiveSemaphoreDao(hiveDataSource).get();
		
		if(this.getRevision() != hs.getRevision()) {
			this.setSemaphore(hs);
			initialize(new PartitionDimensionDao(hiveDataSource).loadAll());
			updated = true;
		}
		return updated;
	}
	
	public void initialize(Collection<PartitionDimension> dimensions) {
		Map<String, PartitionDimension> dimensionMap = new ConcurrentHashMap<String, PartitionDimension>();
		Map<String, Directory> directoryMap = new ConcurrentHashMap<String, Directory>();
		Map<String, ConnectionManager> connectionMap = new ConcurrentHashMap<String, ConnectionManager>();
		
		for (PartitionDimension p : dimensions){
			if(this.getDefaultNodeAssigner() != null)
				p.setAssigner(this.getDefaultNodeAssigner());
			dimensionMap.put(p.getName(), p);
			directoryMap.put(p.getName(), new Directory(p, dataSourceProvider.getDataSource(p.getIndexUri())));
			connectionMap.put(p.getName(), new ConnectionManager(directoryMap.get(p.getName()), dataSourceProvider, this.semaphore));
		}
		
		synchronized (this) {
			this.partitionDimensions = dimensionMap;
			this.directories = directoryMap;
			this.connections = connectionMap;
		}
	}

	protected Hive() {
		this.semaphore = new HiveSemaphore();
		initialize(new ArrayList<PartitionDimension>());
	}
	
	protected Hive(String hiveUri, int revision, boolean readOnly, DataSourceProvider dataSourceProvider) {
		this();
		this.hiveUri = hiveUri;
		this.semaphore.setRevision(revision);
		this.semaphore.setReadOnly(readOnly);
		this.dataSourceProvider = dataSourceProvider;
		this.hiveDataSource = dataSourceProvider.getDataSource(hiveUri);
	}

	public String getUri() {
		return hiveUri;
	}

	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, getRevision(), getPartitionDimensions(), isReadOnly() });
	}

	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	public boolean isReadOnly() {
		return semaphore.isReadOnly();
	}

	public void updateHiveReadOnly(Boolean readOnly) {
		this.semaphore.setReadOnly(readOnly);
		new HiveSemaphoreDao(hiveDataSource).update(this.semaphore);
	}

	public int getRevision() {
		return semaphore.getRevision();
	}
	
	public HiveSemaphore getSemaphore() {
		return semaphore;
	}
	
	public HiveSemaphore setSemaphore(HiveSemaphore semaphore) {
		this.semaphore = semaphore;
		return semaphore;
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

	private void incrementAndPersistHive(DataSource datasource) {
		new HiveSemaphoreDao(datasource).incrementAndPersist();
		this.sync();
	}
	
	public ConnectionManager connection(String dimensionName) {
		return this.connections.get(dimensionName);
	}
	
	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimensions());
	}

	public HiveDbDialect getDialect() {
		return DriverLoader.discernDialect(hiveUri);
	}

	public void update(Observable o, Object arg) {
		if(sync())
			notifyObservers();
	}

	public void notifyObservers() {
		super.setChanged();
		super.notifyObservers();
	}

	public Assigner getDefaultNodeAssigner() {
		return defaultNodeAssigner;
	}

	public void setDefaultNodeAssigner(Assigner defaultNodeAssigner) {
		this.defaultNodeAssigner = defaultNodeAssigner;
	}
	
	public Directory directory(String dimensionName) {
		return this.directories.get(dimensionName);
	}

	public Directory addDirectory(Directory dir) {
		this.directories.put(dir.getPartitionDimension().getName(), dir);
		return dir;
	}
	
	//Configuration functions
	public void updateNodeReadOnly(Node node, Boolean readOnly){
		node.setReadOnly(readOnly);
		try {
			this.updateNode(node);
		} catch (HiveReadOnlyException e) {
			//quash since the hive is already read-only
		}
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
		
		node.setPartitionDimensionId(partitionDimension.getId());
		
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
		
		//Destroy the corresponding ConnectionManager
		this.connections.remove(partitionDimension.getName());
		
		return partitionDimension;
	}

	public Node deleteNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getPartitionDimension(node.getPartitionDimensionId()).getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.delete(node);
		incrementAndPersistHive(hiveDataSource);
		//Synchronize the DataSourceCache
		connections.get(getPartitionDimension(node.getPartitionDimensionId()).getName()).removeNode(node);
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

	//Directory functions
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
		// insertResourceId is a noop if the resource is the same index as the partition dimension
		if (resource.isPartitioningResource()) 
			return; // TODO consider throwing a runtime exception here to disallow this condition
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName).getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(semaphores, this);
		directories.get(dimension.getName()).insertResourceId(resource, id, primaryIndexKey);
	}

	private void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		Collection<KeySemaphore> semaphores = 
			directories.get(partitionDimensionName).getKeySemaphoresOfResourceId(secondaryIndex.getResource(), resourceId);
		Preconditions.isWritable(semaphores, this);
		directories.get(partitionDimensionName)
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						resourceId);
	}

	public void insertSecondaryIndexKey(
			String secondaryIndexName, 
			String resourceName, 
			String partitionDimensionName,
			Object secondaryIndexKey,
			Object resourceId) throws HiveReadOnlyException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				resourceId);
	}
	
	public void insertRelatedSecondaryIndexKeys(String partitionDimensionName, String resourceName, Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException {
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName).getKeySemaphoresOfResourceId(getPartitionDimension(partitionDimensionName).getResource(resourceName), resourceId);
		Preconditions.isWritable(semaphores,this);
		directories.get(partitionDimensionName).batch().insertSecondaryIndexKeys(secondaryIndexValueMap, resourceId);
	}

	public void updatePrimaryIndexReadOnly(String partitionDimensionName,
			Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		Collection<KeySemaphore> semaphores = directories.get(partitionDimensionName). getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(HiveUtils.getNodesForSemaphores(semaphores, partitionDimension));
		directories.get(partitionDimension.getName()).updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
	}

	public void updatePrimaryIndexKeyOfResourceId(String partitionDimensionName,String resourceName, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		Preconditions.isWritable(directories.get(partitionDimensionName).getKeySemamphoresOfPrimaryIndexKey(newPrimaryIndexKey), this);
		final Resource resource = getPartitionDimension(partitionDimensionName).getResource(resourceName);
		if (resource.isPartitioningResource()) 
			throw new HiveRuntimeException(String.format("Resource %s is a partitioning dimension, you cannot update its primary index key because it is the resource id", resource.getName()));
			
		directories.get(partitionDimensionName).updatePrimaryIndexKeyOfResourceId(
				resource, 
				resourceId, 
				originalPrimaryIndexKey, 
				newPrimaryIndexKey);
	}
	
	private void deletePrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveReadOnlyException {

		if (!doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey))
			throw new HiveKeyNotFoundException("The primary index key " + primaryIndexKey
					+ " does not exist",primaryIndexKey);
		
		Preconditions.isWritable(directories.get(partitionDimension.getName()).getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey), this);
		
		Directory directory = directories.get(partitionDimension.getName());
		for (Resource resource : partitionDimension.getResources()){
			if (!resource.isPartitioningResource())
				for(Object resourceId : directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey)) {
					deleteResourceId(resource, resourceId);
				}
			else
				directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, primaryIndexKey);
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
		if (resource.isPartitioningResource())
			throw new HiveRuntimeException(String.format("Attempt to delete a resource id of resource %s, which is a partitioning dimension. It can only be deleted as a primary index key", id));
			
		Directory directory = directories.get(resource.getPartitionDimension().getName());
		Preconditions.isWritable(directory.getKeySemaphoresOfResourceId(resource, id), this);
	
		directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
		directory.deleteResourceId(resource, id);
	}
	
	public void deleteAllSecondaryIndexKeysOfResourceId(String partitionDimensionName, String resourceName, Object id) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		final Resource resource = partitionDimension.getResource(resourceName);
		Directory directory = directories.get(partitionDimensionName);
		
		Preconditions.isWritable(directory.getKeySemaphoresOfResourceId(resource, id),this);
		directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
	}
	
	private void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException{
		String partitionDimensionName = secondaryIndex.getResource().getPartitionDimension().getName();
		
		Preconditions.isWritable(directories.get(partitionDimensionName).getKeySemaphoresOfResourceId(secondaryIndex.getResource(), resourceId),this);
		
		if (!doesSecondaryIndexKeyExist(secondaryIndex.getName(), secondaryIndex.getResource().getName(), partitionDimensionName, secondaryIndexKey, resourceId))
			throw new HiveKeyNotFoundException(
					String.format("Secondary index key %s of secondary index %s does not exist",secondaryIndexKey,secondaryIndex.getName()),secondaryIndexKey);

		directories.get(partitionDimensionName)
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
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
			Object secondaryIndexKey, Object resourceId) {
		SecondaryIndex secondaryIndex = getPartitionDimension(dimensionName).getResource(resourceName).getSecondaryIndex(secondaryIndexName);
		return directories.get(dimensionName)
				.doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey, resourceId);
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

	public Collection<?> getSecondaryIndexKeysWithResourceId(
			String secondaryIndexName,
			String resourceName,
			String partitionDimensionName, 
			Object resourceId) {
		return directories.get(partitionDimensionName).getSecondaryIndexKeysOfResourceId(
				getPartitionDimension(partitionDimensionName).getResource(resourceName)
					.getSecondaryIndex(secondaryIndexName), 
				resourceId);
	}

	public Collection<?> getResourceIdsWithPrimaryKey(
			String resourceName,
			String partitionDimensionName, Object primaryIndexKey) {
		
		final Resource resource = partitionDimensions.get(partitionDimensionName).getResource(resourceName);
		if (resource.isPartitioningResource())
			return Collections.singletonList(primaryIndexKey); // TODO consider throwing a runtime exception here to disallow this condition
		return directories.get(partitionDimensionName).getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey);
	}
	
}
