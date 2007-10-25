/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;

import javax.sql.DataSource;

import org.hivedb.ConnectionManager;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable;
import org.hivedb.Synchronizeable;
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
	
	private ConnectionManager connection;
	private PartitionDimension dimension;
	private Directory directory;
	
	private DataSource hiveDataSource;
	private DataSourceProvider dataSourceProvider;
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

	public static Hive load(String hiveDatabaseUri) {
		return load(hiveDatabaseUri, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT));
	}

	public static Hive load(String hiveDatabaseUri, DataSourceProvider dataSourceProvider) {
		return load(hiveDatabaseUri, dataSourceProvider, null);
	}
	
	public static Hive create(String hiveUri, String dimensionName, int indexType) {
		return create(hiveUri, dimensionName, indexType, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT), null);
	}
	
	public static Hive create(String hiveUri, String dimensionName, int indexType, DataSourceProvider provider) {
		return create(hiveUri, dimensionName, indexType, provider, null);
	}
	
	public static Hive create(String hiveUri, String dimensionName, int indexType, DataSourceProvider provider, Assigner assigner) {
		Hive hive = prepareHive(hiveUri, provider, assigner);
		PartitionDimension dimension = new PartitionDimension(dimensionName, indexType);
		dimension.setIndexUri(hiveUri);
		DataSource ds = provider.getDataSource(hiveUri);
		new PartitionDimensionDao(ds).create(dimension);
		new IndexSchema(dimension).install();
		hive.incrementAndPersistHive(ds);
		return hive;
	}

	public static Hive load(String hiveUri, DataSourceProvider provider, Assigner assigner) {
		Hive hive = prepareHive(hiveUri, provider, assigner);
		hive.sync();
		return hive;
		
	}
	
	private static Hive prepareHive(String hiveUri, DataSourceProvider provider, Assigner assigner) {
		DriverLoader.initializeDriver(hiveUri);
		Hive hive = new Hive(hiveUri, 0, false, provider);
		if(assigner != null)
			hive.setAssigner(assigner);
		return hive;
	}

	public boolean sync() {
		boolean updated = false;
		HiveSemaphore hs = new HiveSemaphoreDao(hiveDataSource).get();
		
		if(this.getRevision() != hs.getRevision()) {
			this.setSemaphore(hs);
			initialize(new PartitionDimensionDao(hiveDataSource).get());
			updated = true;
		}
		return updated;
	}
	
	public void initialize(PartitionDimension dimension) {
		Directory directory = new Directory(dimension, dataSourceProvider.getDataSource(dimension.getIndexUri()));
		ConnectionManager connection = new ConnectionManager(directory, dataSourceProvider, this.semaphore);
		
		synchronized (this) {
			this.dimension = dimension;
			this.directory = directory;
			this.connection = connection;
		}
	}

	protected Hive() {
		this.semaphore = new HiveSemaphore();
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
	

	public DataSourceProvider getDataSourceProvider() {
		return dataSourceProvider;
	}

	public void setDataSourceProvider(DataSourceProvider dataSourceProvider) {
		this.dataSourceProvider = dataSourceProvider;
	} 

	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, getRevision(), dimension, isReadOnly() });
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
	
	public PartitionDimension getPartitionDimension() {
		return this.dimension;
	}
	
	public PartitionDimension setPartitionDimension(PartitionDimension dimension) {
		this.dimension = dimension;
		incrementAndPersistHive(hiveDataSource);
		sync();
		return getPartitionDimension();
	}

	private void incrementAndPersistHive(DataSource datasource) {
		new HiveSemaphoreDao(datasource).incrementAndPersist();
		this.sync();
	}
	
	public ConnectionManager connection() {
		return this.connection;
	}
	
	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimension());
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

	public Assigner getAssigner() {
		return assigner;
	}

	public void setAssigner(Assigner assigner) {
		this.assigner = assigner;
	}
	
	public Directory directory() {
		return this.directory;
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

	public Node addNode(Node node)
			throws HiveReadOnlyException {
		
		node.setPartitionDimensionId(dimension.getId());
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(dimension.getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.create(node);

		incrementAndPersistHive(hiveDataSource);
		return node;
	}

	public Resource addResource(Resource resource) throws HiveReadOnlyException{
		resource.setPartitionDimension(dimension);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(dimension.getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.create(resource);
		incrementAndPersistHive(hiveDataSource);
		return dimension.getResource(resource.getName());
	}

	public SecondaryIndex addSecondaryIndex(Resource resource, SecondaryIndex secondaryIndex) throws HiveReadOnlyException{
		secondaryIndex.setResource(resource);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(resource.getSecondaryIndexes(), secondaryIndex);
		
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		new IndexSchema(dimension).install();
		return secondaryIndex;
	}

	public PartitionDimension updatePartitionDimension(PartitionDimension partitionDimension) throws HiveReadOnlyException  {
		Preconditions.isWritable(this);

		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
		
		return partitionDimension;
	}

	public Node updateNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(dimension.getNodes(), node);

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

	public Node deleteNode(Node node) throws HiveReadOnlyException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(dimension.getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.delete(node);
		incrementAndPersistHive(hiveDataSource);
		//Synchronize the DataSourceCache
		connection.removeNode(node);
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
	public void insertPrimaryIndexKey(Object primaryIndexKey) throws HiveReadOnlyException {
		Node node = assigner.chooseNode(dimension.getNodes(), primaryIndexKey);
		Preconditions.isWritable(this, node);
		directory.insertPrimaryIndexKey(node, primaryIndexKey);
	}

	public void insertResourceId(String resourceName, Object id, Object primaryIndexKey) throws HiveReadOnlyException{
		Resource resource = dimension.getResource(resourceName);
		// insertResourceId is a noop if the resource is the same index as the partition dimension
		if (resource.isPartitioningResource()) 
			return; // TODO consider throwing a runtime exception here to disallow this condition
		Collection<KeySemaphore> semaphores = directory.getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(semaphores, this);
		directory.insertResourceId(resource, id, primaryIndexKey);
	}

	private void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		Collection<KeySemaphore> semaphores = 
			directory.getKeySemaphoresOfResourceId(secondaryIndex.getResource(), resourceId);
		Preconditions.isWritable(semaphores, this);
		directory.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,resourceId);
	}

	public void insertSecondaryIndexKey(
			String secondaryIndexName, 
			String resourceName, 
			Object secondaryIndexKey,
			Object resourceId) throws HiveReadOnlyException {
		insertSecondaryIndexKey(dimension
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				resourceId);
	}
	
	public void insertRelatedSecondaryIndexKeys(String resourceName, Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveReadOnlyException {
		Collection<KeySemaphore> semaphores = directory.getKeySemaphoresOfResourceId(dimension.getResource(resourceName), resourceId);
		Preconditions.isWritable(semaphores,this);
		directory.batch().insertSecondaryIndexKeys(secondaryIndexValueMap, resourceId);
	}

	public void updatePrimaryIndexReadOnly(Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException {
		Collection<KeySemaphore> semaphores = directory. getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey);
		Preconditions.isWritable(HiveUtils.getNodesForSemaphores(semaphores, dimension));
		directory.updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
	}

	public void updatePrimaryIndexKeyOfResourceId(String resourceName, Object resourceId, Object originalPrimaryIndexKey, Object newPrimaryIndexKey) throws HiveReadOnlyException {
		Preconditions.isWritable(directory.getKeySemamphoresOfPrimaryIndexKey(newPrimaryIndexKey), this);
		final Resource resource = dimension.getResource(resourceName);
		if (resource.isPartitioningResource()) 
			throw new HiveRuntimeException(String.format("Resource %s is a partitioning dimension, you cannot update its primary index key because it is the resource id", resource.getName()));
			
		directory.updatePrimaryIndexKeyOfResourceId(
				resource, 
				resourceId, 
				originalPrimaryIndexKey, 
				newPrimaryIndexKey);
	}
	
	public void deletePrimaryIndexKey(Object primaryIndexKey) throws HiveReadOnlyException {

		if (!doesPrimaryIndexKeyExist(primaryIndexKey))
			throw new HiveKeyNotFoundException("The primary index key " + primaryIndexKey
					+ " does not exist",primaryIndexKey);
		
		Preconditions.isWritable(directory.getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey), this);
		
		for (Resource resource : dimension.getResources()){
			if (!resource.isPartitioningResource())
				for(Object resourceId : directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey)) {
					deleteResourceId(resource, resourceId);
				}
			else
				directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, primaryIndexKey);
		}
		directory.deletePrimaryIndexKey(primaryIndexKey);
	}

	public void deleteResourceId(String resourceName, Object id) throws HiveReadOnlyException {
		deleteResourceId(dimension.getResource(resourceName), id);
	}
	
	private void deleteResourceId(final Resource resource, Object id) throws HiveReadOnlyException {
		if (resource.isPartitioningResource())
			throw new HiveRuntimeException(String.format("Attempt to delete a resource id of resource %s, which is a partitioning dimension. It can only be deleted as a primary index key", id));
			
		Preconditions.isWritable(directory.getKeySemaphoresOfResourceId(resource, id), this);
	
		directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
		directory.deleteResourceId(resource, id);
	}
	
	public void deleteAllSecondaryIndexKeysOfResourceId(String resourceName, Object id) throws HiveReadOnlyException {
		PartitionDimension partitionDimension = dimension;
		final Resource resource = partitionDimension.getResource(resourceName);
		
		Preconditions.isWritable(directory.getKeySemaphoresOfResourceId(resource, id),this);
		directory.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, id);
	}
	
	private void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException{
		
		Preconditions.isWritable(directory.getKeySemaphoresOfResourceId(secondaryIndex.getResource(), resourceId),this);
		if (!doesSecondaryIndexKeyExist(secondaryIndex.getName(), secondaryIndex.getResource().getName(), secondaryIndexKey, resourceId))
			throw new HiveKeyNotFoundException(
					String.format("Secondary index key %s of secondary index %s does not exist",secondaryIndexKey,secondaryIndex.getName()),secondaryIndexKey);

		directory
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey, resourceId);
	}

	//TODO: Fix parameter ordering
	public void deleteSecondaryIndexKey(String secondaryIndexName, String resourceName,
			Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException {
		deleteSecondaryIndexKey(dimension
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey, resourceId);
	}

	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
		return directory.doesPrimaryIndexKeyExist(
				primaryIndexKey);
	}

	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey) {
		Boolean readOnly = directory.getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
		if (readOnly != null)
			return readOnly;
		throw new HiveKeyNotFoundException(String.format(
				"Primary index key %s of partition dimension %s not found.",
				primaryIndexKey.toString(), dimension.getName()),primaryIndexKey);
	}

	public boolean getReadOnlyOfResourceId(String resourceName, Object id) {
		return directory
			.getReadOnlyOfResourceId(dimension
					.getResource(resourceName), id);
	}

	//TODO Parameter ordering
	public boolean doesSecondaryIndexKeyExist(String secondaryIndexName, String resourceName,
			Object secondaryIndexKey, Object resourceId) {
		SecondaryIndex secondaryIndex = dimension.getResource(resourceName).getSecondaryIndex(secondaryIndexName);
		return directory
				.doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey, resourceId);
	}

	public boolean doesResourceIdExist(String resourceName, Object id) {
		return directory.doesResourceIdExist(
				dimension.getResource(resourceName), id);
	}

	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		Collection<Object> primaryIndexKeys = directory
				.getPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex,
						secondaryIndexKey);
		if (primaryIndexKeys.size() > 0)
			return primaryIndexKeys;
		else
			throw new HiveKeyNotFoundException(
				String
						.format(
								"Secondary index key %s of partition dimension %s on secondary index %s not found.",
								secondaryIndexKey.toString(), dimension
										.getName(), secondaryIndex.getName()), primaryIndexKeys);
	}

	//TODO Stringify params
	@SuppressWarnings("unchecked")
	public Collection<Object> getResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
		return directory.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
	}

	public Collection<?> getSecondaryIndexKeysWithResourceId(
			String secondaryIndexName,
			String resourceName,
			Object resourceId) {
		return directory.getSecondaryIndexKeysOfResourceId(
				dimension.getResource(resourceName)
					.getSecondaryIndex(secondaryIndexName), 
				resourceId);
	}

	public Collection<?> getResourceIdsWithPrimaryKey(String resourceName, Object primaryIndexKey) {
		final Resource resource = dimension.getResource(resourceName);
		if (resource.isPartitioningResource())
			return Collections.singletonList(primaryIndexKey); // TODO consider throwing a runtime exception here to disallow this condition
		return directory.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey);
	}
	
}
