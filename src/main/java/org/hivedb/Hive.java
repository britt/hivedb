/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.Lockable.Status;
import org.hivedb.meta.*;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryFacade;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.persistence.*;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.Preconditions;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

import javax.sql.DataSource;
import java.util.*;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 * 
 * The facade for all fundamental CRUD operations on the Hive directories and Hive metadata.
 */
public class Hive extends Observable implements Synchronizeable, Observer, Lockable, Finder, Nameable, HiveFacade {
	//constants
	private static final int DEFAULT_JDBC_TIMEOUT = 500;
	public static final int NEW_OBJECT_ID = 0;
	
	private HiveSemaphore semaphore;
	private String hiveUri;
	private ConnectionManager connection;
	private PartitionDimension dimension;
	private DirectoryFacade directory;
	private Collection<Node> nodes = new ArrayList<Node>();
	
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

	/**
	 *  Calls {@see #load(String, DataSourceProvider, Assigner)} with the
	 *  default DataSourceProvider and Assigner.
	 */
	public static Hive load(String hiveDatabaseUri) {
		return load(hiveDatabaseUri, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT));
	}

	/**
	 *  Calls {@see #load(String, DataSourceProvider, Assigner)} with the
	 *  default DataSourceProvider.
	 */
	public static Hive load(String hiveDatabaseUri, DataSourceProvider dataSourceProvider) {
		return load(hiveDatabaseUri, dataSourceProvider, null);
	}

	/**
	 *  Loads an existing hive at the given location using the given DataSourceProvider and
	 *  default Assigner. <b>Side Effect:</b> creates a HiveSemaphore that keeps the Hive
	 *  instance synchronized with the hive database metadata. {@see org.hivedb.meta.HiveSemaphore#}
	 * @param hiveDatabaseUri The URI of an existing hive configuration.
	 * @param provider The DataSourceProvider used to resolve the DataSource of a data node.
	 * @param assigner The key assigner to be used by the Hive for identifying new unidentified entity instances
	 * @return a Hive instance
	 */
	public static Hive load(String hiveUri, DataSourceProvider provider, Assigner assigner) {
		Hive hive = prepareHive(hiveUri, provider, assigner);
		hive.sync();
		return hive;
		
	}
	
	/**
	 * Calls {@see #create(String, String, int, DataSourceProvider, Assigner)} with the default
	 * DataSourceProvider and Assigner.
	 */
	public static Hive create(String hiveUri, String dimensionName, int indexType) {
		return create(hiveUri, dimensionName, indexType, new HiveBasicDataSourceProvider(DEFAULT_JDBC_TIMEOUT), null);
	}
	
	/**
	  * Calls {@see #create(String, String, int, DataSourceProvider, Assigner)} with the default
	 * DataSourceProvider.
	 */
	public static HiveFacade create(String hiveUri, String dimensionName, int indexType, DataSourceProvider provider) {
		return create(hiveUri, dimensionName, indexType, provider, null);
	}
	
	/**
	 * Creates a new Hive. Primary caller: {@see org.hivedb.hibernate.ConfigurationReader#install(HiveFacade)}
	 * @param hiveUri - The location of the hive database
	 * @param dimensionName - The name of the hive's partitioning dimension, used for naming hive index tables.
	 * @param indexType - The ___ indicating the type of the primary index of the partion dimension.
	 * @param provider - The DataSourceProvider used to resolve the DataSource of a data node.
	 * @param assigner - The key assigner to be used by the Hive for identifying new unidentified entity instances.
	 * @return an instance to access the created hive.
	 */
	public static Hive create(String hiveUri, String dimensionName, int indexType, DataSourceProvider provider, Assigner assigner) {
		Hive hive = prepareHive(hiveUri, provider, assigner);
		PartitionDimension dimension = new PartitionDimension(dimensionName, indexType);
		dimension.setIndexUri(hiveUri);
		DataSource ds = provider.getDataSource(hiveUri);
		PartitionDimensionDao dao = new PartitionDimensionDao(ds);
		final List<PartitionDimension> partitionDimensions = dao.loadAll();
		if(partitionDimensions.size() == 0) {
			dao.create(dimension);
			new IndexSchema(dimension).install();
			hive.incrementAndPersistHive(ds);
			return hive;
		} else
			throw new HiveRuntimeException(String.format("There is already a Hive with a partition dimension named %s intalled at this uri: %s", Atom.getFirstOrThrow(partitionDimensions).getName(), hiveUri));
	}
	
	private static Hive prepareHive(String hiveUri, DataSourceProvider provider, Assigner assigner) {
		DriverLoader.initializeDriver(hiveUri);
		Hive hive = new Hive(hiveUri, 0, Status.writable, provider);
		if(assigner != null)
			hive.setAssigner(assigner);
		return hive;
	}

	/**
	 *  Used internally to synchronize the loaded Hive instance to the current state of the Hive database metadata.
	 */
	public boolean sync() {
		boolean updated = false;
		HiveSemaphore hs = new HiveSemaphoreDao(hiveDataSource).get();
		
		if(this.getRevision() != hs.getRevision()) {
			this.setSemaphore(hs);
			initialize(hiveDataSource);
			updated = true;
		}
		return updated;
	}
	
	/**
	 *  {@see #sync()} 
	 * @return
	 */
	public boolean forceSync() {
		initialize(hiveDataSource);
		return true;
	}
	
	private void initialize(DataSource ds) {
		//Always fetch and add nodes
		Collection<Node> nodes = new NodeDao(hiveDataSource).loadAll();
		synchronized (this) {
			this.nodes = nodes;
		}
		
		//Only synchronize other properties if a Partition Dimension exists
		try {
			PartitionDimension dimension = new PartitionDimensionDao(ds).get();
			DirectoryFacade directory = new DirectoryWrapper(dimension, ds, getAssigner(), this);
			synchronized (this) {
				ConnectionManager connection = new ConnectionManager(new Directory(dimension, ds), this, dataSourceProvider);
				this.dimension = dimension;
				this.directory = directory;
				this.connection = connection;
			}
		} catch( HiveRuntimeException e) {
			//quash
		}
        
	}
	

	protected Hive() {
		this.semaphore = new HiveSemaphore();
	}
	
	protected Hive(String hiveUri, int revision, Status status, DataSourceProvider dataSourceProvider) {
		this();
		this.hiveUri = hiveUri;
		this.semaphore.setRevision(revision);
		this.semaphore.setStatus(status);
		this.dataSourceProvider = dataSourceProvider;
		this.hiveDataSource = dataSourceProvider.getDataSource(hiveUri);
	}

	/**
	 * {@inheritDoc} 
	 */
	public String getUri() {
		return hiveUri;
	}
	
	/**
	 * {@inheritDoc} 
	 */
	public DataSourceProvider getDataSourceProvider() {
		return dataSourceProvider;
	}

	/**
	 * Sets the dataSourceProvider for testing purposes. Use {@see #load(String, DataSourceProvider, Assigner)} instead.
	 * @param dataSourceProvider
	 */
	public void setDataSourceProvider(DataSourceProvider dataSourceProvider) {
		this.dataSourceProvider = dataSourceProvider;
	} 

	/**
	 * Hashes the Hive based on hiveUri, revision, partition dimension
	 */
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] { hiveUri, getRevision(), dimension });
	}

	/**
	 * Tests equality with {@see #hashCode()}
	 */
	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	public Status getStatus() {
		return semaphore.getStatus();
	}
	
	/**
	 * Updates the Hive to new lockable status
	 * @param status
	 */
	public void updateHiveStatus(Status status) {
		this.semaphore.setStatus(status);
		new HiveSemaphoreDao(hiveDataSource).update(this.semaphore);
	}
	
	/**
	 * {@inheritDoc} 
	 */
	public int getRevision() {
		return semaphore.getRevision();
	}
	
	/**
	 * {@inheritDoc} 
	 */
	public HiveSemaphore getSemaphore() {
		return semaphore;
	}
	
	public HiveSemaphore setSemaphore(HiveSemaphore semaphore) {
		this.semaphore = semaphore;
		return semaphore;
	}

	/**
	 * {@inheritDoc} 
	 */
	public PartitionDimension getPartitionDimension() {
		return this.dimension;
	}
	
	/**
	 * TODO does this method correctly update the hive metadata? It doesn't appear to.
	 * 
	 * @param dimension
	 * @return
	 */
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
	
	/**
	 * Gets the ConnectionManager, probably only useful for testing.
	 * @return
	 */
	public ConnectionManager connection() {
		return this.connection;
	}
	
	/**e
	 * Dumps the properties of the Hive.
	 */
	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimension());
	}

	/** 
	 * {@inheritDoc}
	 */
	public HiveDbDialect getDialect() {
		return DriverLoader.discernDialect(hiveUri);
	}
	
	/** 
	 * {@inheritDoc}
	 */
	public void update(Observable o, Object arg) {
		if(sync())
			notifyObservers();
	}

	/** 
	 * {@inheritDoc}
	 */
	public void notifyObservers() {
		super.setChanged();
		super.notifyObservers();
	}

	/** 
	 * {@inheritDoc}
	 */
	public Assigner getAssigner() {
		return assigner;
	}

	/**
	 * Sets the assigner. Use {@see #load(String, DataSourceProvider, Assigner)} instead.
	 * @param assigner
	 */
	public void setAssigner(Assigner assigner) {
		this.assigner = assigner;
	}
	
	/**
	 * Exposes the DirectoryFacade for hive index operations. See {@see org.hivedb.meta.directory.DirectoryFacade}
	 * @return
	 */
	public DirectoryFacade directory() {
		return this.directory;
	}
	
	//Configuration functions
		public void updateNodeStatus(Node node, Status status){
			node.setStatus(status);
			try {
				this.updateNode(node);
			} catch (HiveLockableException e) {
				//quash since the hive is already read-only
			}
		}


	/** 
	 * {@inheritDoc}
	 */
	public Node addNode(Node node)
			throws HiveLockableException {
				
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.create(node);

		incrementAndPersistHive(hiveDataSource);
		return node;
	}
	
	/** 
	 * {@inheritDoc}
	 */
	public Collection<Node> addNodes(Collection<Node> nodes) throws HiveLockableException {
		Preconditions.isWritable(this);

		for(Node node : nodes) {
			Preconditions.nameIsUnique(getNodes(), node);
			NodeDao nodeDao = new NodeDao(hiveDataSource);
			nodeDao.create(node);
		}
		
		incrementAndPersistHive(hiveDataSource);
		return nodes;
	}

	/** 
	 * {@inheritDoc}
	 */
	public boolean doesResourceExist(String resourceName) {
		return !Preconditions.isNameUnique(dimension.getResources(), resourceName);
	}
	
	/** 
	 * {@inheritDoc}
	 */
	public Resource addResource(Resource resource) throws HiveLockableException{
		resource.setPartitionDimension(dimension);
		
		Preconditions.isWritable(this);
		Preconditions.isNameUnique(dimension.getResources(), resource.getName());

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.create(resource);
		incrementAndPersistHive(hiveDataSource);
		new IndexSchema(dimension).install();
		return dimension.getResource(resource.getName());
	}

	/** 
	 * {@inheritDoc}
	 */
	public SecondaryIndex addSecondaryIndex(Resource resource, SecondaryIndex secondaryIndex) throws HiveLockableException{
		secondaryIndex.setResource(resource);
		
		Preconditions.isWritable(this);
		Preconditions.nameIsUnique(resource.getSecondaryIndexes(), secondaryIndex);
		
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.create(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		new IndexSchema(dimension).install();
		return secondaryIndex;
	}

	/**
	 * Updates the Hive's PartitionDimension to the given instance. The hive partition_dimension_metadata
	 * table is updated immediately to reflect any changes. Note that changes to nodes are not
	 * updated by this call and must be updated explicitly. See {@see #updateNod(Node)}.
	 * This method increments the hive version. 
	 * @param partitionDimension
	 * @return the given PartitionDimension
	 * @throws HiveLockableException
	 */
	public PartitionDimension updatePartitionDimension(PartitionDimension partitionDimension) throws HiveLockableException  {
		Preconditions.isWritable(this);

		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
		partitionDimensionDao.update(partitionDimension);
		incrementAndPersistHive(hiveDataSource);
		
		return partitionDimension;
	}
	
	/**
	 * Updates the Node of the hive matching the id of the given Node to the given instance.
	 * The hive node_metadata table is updated immediately to reflect any changes, and the hive version is incremented.
	 * @param partitionDimension
	 * @return the given PartitionDimension
	 * @throws HiveLockableException
	 */
	public Node updateNode(Node node) throws HiveLockableException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getNodes(), node);

		new NodeDao(hiveDataSource).update(node);
		incrementAndPersistHive(hiveDataSource);
		return node;
	}

	/**
	 * Updates the Hive Resource matching the id of the given instance. The hive resource_metadata
	 * table is updated immediately to reflect any changes. This method increments the hive version. 
	 * @param resource
	 * @return the given Resource
	 * @throws HiveLockableException
	 */
	public Resource updateResource(Resource resource) throws HiveLockableException  {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(resource.getPartitionDimension().getResources(), resource);
		Preconditions.nameIsUnique(resource.getPartitionDimension().getResources(), resource);

		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.update(resource);
		incrementAndPersistHive(hiveDataSource);

		return resource;
	}
	
	/**
	 * Updates the Hive SecondaryIndex matching the id of the given instance. The hive secondary_index_metadata
	 * table is updated immediately to reflect any changes. This method increments the hive version. 
	 * @param resource
	 * @return the given Resource
	 * @throws HiveLockableException
	 */
	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveLockableException  {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);
		Preconditions.nameIsUnique(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryIndexDao.update(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);

		return secondaryIndex;
	}

	/** 
	 * {@inheritDoc}
	 */
	public Node deleteNode(Node node) throws HiveLockableException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(getNodes(), node);
		
		NodeDao nodeDao = new NodeDao(hiveDataSource);
		nodeDao.delete(node);
		incrementAndPersistHive(hiveDataSource);
		//Synchronize the DataSourceCache
		connection.removeNode(node);
		return node;
	}

	/** 
	 * {@inheritDoc}
	 */
	public Resource deleteResource(Resource resource) throws HiveLockableException {
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(resource.getPartitionDimension().getResources(), resource);
		
		ResourceDao resourceDao = new ResourceDao(hiveDataSource);
		resourceDao.delete(resource);
		incrementAndPersistHive(hiveDataSource);
		
		return resource;
	}
	
	/** 
	 * {@inheritDoc}
	 */
	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveLockableException{
		Preconditions.isWritable(this);
		Preconditions.idIsPresentInList(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(hiveDataSource);
		secondaryindexDao.delete(secondaryIndex);
		incrementAndPersistHive(hiveDataSource);
		
		return secondaryIndex;
	}	
	
	/** 
	 * {@inheritDoc}
	 */
	public Collection<Node> getNodes() {
		return nodes;
	}
	
	/** 
	 * {@inheritDoc}
	 */
	public Node getNode(final String name) {
		return Filter.grepSingle(new Predicate<Node>(){
			public boolean f(Node item) {
				return item.getName().equalsIgnoreCase(name);
			}}, getNodes());
	}
	
	/**
	 * Returns the node with the given id
	 * @param id
	 * @return
	 */
	public Node getNode(final int id) {
		return Filter.grepSingle(new Predicate<Node>(){
			public boolean f(Node item) {
				return item.getId() == id;
			}}, getNodes());
	}

	/**
	 * For internal use only.
	 */
	@SuppressWarnings("unchecked")
	public<T extends Nameable> T findByName(Class<T> forClass, final String name){
		if (forClass.equals(Node.class))
			return (T)getNode(name);
		
		throw new RuntimeException("Invalid type " + forClass.getName());
	}
	/**
	 * For internal use only.
	 */
	@SuppressWarnings("unchecked")
	public<T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if (forClass.equals(Node.class))
			return (Collection<T>)getNodes();
		throw new RuntimeException("Invalid type " + forClass.getName());
	}

	/**
	 * returns the name of the hive, "hive"
	 */
	public String getName() {
		return "hive";
	}
}
