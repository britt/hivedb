/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.meta.*;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryFacade;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.persistence.*;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.Preconditions;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;

import javax.sql.DataSource;
import java.util.*;

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
	private DirectoryFacade directory;
	
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

	public static Hive load(String hiveUri, DataSourceProvider provider, Assigner assigner) {
		Hive hive = prepareHive(hiveUri, provider, assigner);
		hive.sync();
		return hive;
		
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
	
	public boolean forceSync() {
		initialize(new PartitionDimensionDao(hiveDataSource).get());
		return true;
	}
	
	public void initialize(PartitionDimension dimension) {
        DataSource dataSource = dataSourceProvider.getDataSource(dimension.getIndexUri());
        DirectoryFacade directory = new DirectoryWrapper(dimension, dataSource, getAssigner(), getSemaphore());
		ConnectionManager connection = new ConnectionManager(new Directory(dimension, dataSource), dataSourceProvider, this.semaphore);
		
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
	
	public DirectoryFacade directory() {
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
}
