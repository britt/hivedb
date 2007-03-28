/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.hivedb.HiveDbDialect;
import org.hivedb.HiveException;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.management.statistics.HivePerformanceStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.DriverLoader;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.InstallHiveGlobalSchema;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class Hive implements Finder {
	private static Logger log = Logger.getLogger(Hive.class);

	public static final int NEW_OBJECT_ID = 0;

	public static String URI_SYSTEM_PROPERTY = "org.hivedb.uri";

	private String hiveUri;

	private int revision;

	private boolean readOnly;

	private Collection<PartitionDimension> partitionDimensions;

	private PartitionKeyStatisticsDao statistics;

	private Collection<Directory> directories;

	private HiveSyncDaemon daemon;

	private DataSource dataSource;

//	private HivePerformanceStatistics stats;
	
	/**
	 * System entry point. Factory method for all Hive interaction. If the first
	 * attempt to load the Hive throws any exception, the Hive class will
	 * attempt to install the database schema at the target URI, and then
	 * attempt to reload that schema one time before throwing a HiveException.
	 *
	 * @param hiveDatabaseUri
	 *            Target hive
	 * @return Hive (existing or new) located at hiveDatabaseUri
	 * @throws HiveException
	 */
	public static Hive load(String hiveDatabaseUri) throws HiveException {
		if (log.isDebugEnabled())
			log.debug("Loading Hive from " + hiveDatabaseUri);
		try {
			DriverLoader.loadByDialect(DriverLoader
					.discernDialect(hiveDatabaseUri));
		} catch (ClassNotFoundException e) {
			throw new HiveException("Unable to load database driver: "
					+ e.getMessage(), e);
		}

		Hive hive = null;
		try {
			PartitionKeyStatisticsDao tracker = new PartitionKeyStatisticsDao(
					new HiveBasicDataSource(hiveDatabaseUri));
			hive = new Hive(hiveDatabaseUri, 0, false,
					new ArrayList<PartitionDimension>(), tracker);
			hive.sync();
			if (log.isDebugEnabled())
				log.debug("Successfully loaded Hive from " + hiveDatabaseUri);
		} catch (Exception ex) {
			log
					.warn("No HiveDB global schema detected (" + ex.getMessage() +  "), attempting installation to "
							+ hiveDatabaseUri);
			for (StackTraceElement s : ex.getStackTrace())
				log.warn(s.toString());
			
			try {
				InstallHiveGlobalSchema.install(hiveDatabaseUri);
				PartitionKeyStatisticsDao tracker = new PartitionKeyStatisticsDao(
						new HiveBasicDataSource(hiveDatabaseUri));
				hive = new Hive(hiveDatabaseUri, 0, false,
						new ArrayList<PartitionDimension>(), tracker);
				if (log.isDebugEnabled())
					log.debug("Successfully installed Hive to "
							+ hiveDatabaseUri);
			} catch (Exception ex2) {
				final String errorMessage = "Unable to load Hive from or install Hive to "
						+ hiveDatabaseUri;
				log.fatal(errorMessage);
				throw new HiveException(errorMessage + ": " + ex2.getMessage(),
						ex2);
			}
		}
		return hive;
	}

	public void create() throws HiveException, SQLException {
		for (PartitionDimension partitionDimension : this
				.getPartitionDimensions())
			new IndexSchema(partitionDimension).install();
	}

	/**
	 * Explicitly syncs the hive with the persisted data, rather than waiting
	 * for the periodic sync.
	 * 
	 * @throws HiveException
	 * 
	 */
	public void sync() throws HiveException {
		daemon.forceSynchronize();
	}

	/**
	 * INTERNAL USE ONLY- load the Hive from persistence
	 * 
	 * @param revision
	 * @param readOnly
	 */
	protected Hive(String hiveUri, int revision, boolean readOnly,
			Collection<PartitionDimension> partitionDimensions,
			PartitionKeyStatisticsDao statistics) {
		this.hiveUri = hiveUri;
		this.revision = revision;
		this.readOnly = readOnly;
		this.partitionDimensions = partitionDimensions;
		this.statistics = statistics;
		this.daemon = new HiveSyncDaemon(this);
		this.dataSource = new HiveBasicDataSource(this.getHiveUri());

		this.directories = new ArrayList<Directory>();
		for (PartitionDimension dimension : this.partitionDimensions)
			this.directories.add(new Directory(dimension, this.dataSource));
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
		return HiveUtils.makeHashCode(new Object[] { hiveUri, revision,
				getPartitionDimensions(), readOnly });
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
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public void updateHiveReadOnly(Boolean readOnly) throws HiveException {
		this.setReadOnly(readOnly);
		try {
			daemon.setReadOnly(isReadOnly());
		} catch (SQLException e) {
			throw new HiveException(
					"Could not change the readonly status of the hive", e);
		}
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
	public void setRevision(int revision) {
		this.revision = revision;
	}

	/**
	 * Gets all partition dimensions of the hive. A PartitionDimension instance
	 * references all of its underlying components--its NodeGroup and Resources.
	 * 
	 * @return
	 */
	public Collection<PartitionDimension> getPartitionDimensions() {
		return partitionDimensions;
	}

	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		return (Collection<T>) getPartitionDimensions();
	}

	/**
	 * Gets a partition dimension by name.
	 * 
	 * @param name
	 *            The user-defined name of a partition dimension
	 * @return
	 * @throws IllegalArgumentException
	 *             Thrown if no parition dimension with the given name exists.
	 *             To avoid this exception, test for existence first with
	 */
	public PartitionDimension getPartitionDimension(String name) {
		PartitionDimension dimension = getPartitionDimensionUnchecked(name);
		if (dimension == null)
			throw new IllegalArgumentException("PartitionDimension with name "
					+ name + " not found.");
		return dimension;
	}
	
	/**
	 * Test for existence of this partition dimension name
	 * @param name Name of partition dimension
	 * @return True if partition dimension exists
	 */
	public boolean containsPartitionDimension(String name) {
		return getPartitionDimensionUnchecked(name) != null;
	}

	private PartitionDimension getPartitionDimensionUnchecked(String name) {
		for (PartitionDimension partitionDimension : getPartitionDimensions())
			if (partitionDimension.getName().equals(name))
				return partitionDimension;
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, String name)
			throws HiveException {
		return (T) getPartitionDimension(name);
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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the data, if the
	 *             hive is currently read-only, or if a partition dimension of
	 *             the same name already exists
	 */
	public PartitionDimension addPartitionDimension(
			PartitionDimension partitionDimension) throws HiveException {
		isWritable("Creating a new partition dimension");
		isUnique(String.format("Partition dimension %s already exists",
				partitionDimension.getName()), getPartitionDimensions(),
				partitionDimension);

		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(
				datasource);
		try {
			partitionDimensionDao.create(partitionDimension);
		} catch (SQLException e) {
			throw new HiveException(
					"Problem persisting new Partition Dimension: "
							+ e.getMessage());
		}
		incrementAndPersistHive(datasource);

		if (partitionDimension.getIndexUri() == null)
			partitionDimension.setIndexUri(this.hiveUri);
		this.directories
				.add(new Directory(partitionDimension, this.dataSource));
		sync();
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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the data, or if the
	 *             hive is currently read-only.
	 */
	public Node addNode(PartitionDimension partitionDimension, Node node)
			throws HiveException {
		node.setNodeGroup(partitionDimension.getNodeGroup());
		isWritable("Creating a new node");

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		NodeDao nodeDao = new NodeDao(datasource);
		try {
			nodeDao.create(node);
		} catch (SQLException e) {
			throw new HiveException("Problem persisting new Node: "
					+ e.getMessage());
		}

		incrementAndPersistHive(datasource);

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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the data, if the
	 *             hive is currently read-only, or if a resource of the same
	 *             name already exists
	 */
	public Resource addResource(PartitionDimension partitionDimension,
			Resource resource) throws HiveException {
		resource.setPartitionDimension(partitionDimension);
		isWritable("Creating a new resource");
		isUnique(String.format(
				"Resource %s already exists in the partition dimension %s",
				resource.getName(), partitionDimension.getName()),
				partitionDimension.getResources(), resource);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		try {
			resourceDao.create(resource);
		} catch (SQLException e) {
			throw new HiveException("Problem persisting new Node: "
					+ e.getMessage());
		}
		incrementAndPersistHive(datasource);

		partitionDimension.getResources().add(resource);
		getPartitionDimensions().add(partitionDimension);
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
	 * @throws HiveException
	 *             Throws if there is a problem persisting the data, the hive is
	 *             read-only, or if a secondary index with the same columnInfo()
	 *             name already exists in the resource.
	 */
	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveException {
		secondaryIndex.setResource(resource);
		isWritable("Creating a new secondary index");
		isUnique(String.format(
				"Secondary index %s already exists in the resource %s",
				secondaryIndex.getName(), resource.getName()), resource
				.getSecondaryIndexes(), secondaryIndex);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(datasource);
		try {
			secondaryIndexDao.create(secondaryIndex);
		} catch (SQLException e) {
			throw new HiveException("Problem persisting new Node: "
					+ e.getMessage());
		}
		incrementAndPersistHive(datasource);
		sync();

		try {
			create();
		} catch (SQLException e) {

			throw new HiveException("Problem persisting new Node: "
					+ e.getMessage());
		}
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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the updates, if the
	 *             hive is currently read-only, or if a partition dimension of
	 *             the same name already exists
	 */
	public PartitionDimension updatePartitionDimension(
			PartitionDimension partitionDimension) throws HiveException {
		isWritable("Updating partition dimension");
		isIdPresentInCollection(String.format(
				"Partition dimension with id %s does not exist",
				partitionDimension.getId()), getPartitionDimensions(),
				partitionDimension);
		isUnique(String.format(
				"Partition dimension with name %s already exists",
				partitionDimension.getName()), getPartitionDimensions(),
				partitionDimension);

		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(
				datasource);
		try {
			partitionDimensionDao.update(partitionDimension);
		} catch (SQLException e) {
			throw new HiveException("Problem updating the partition dimension",
					e);
		}
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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the data, or if the
	 *             hive is currently read-only.
	 */
	public Node updateNode(Node node) throws HiveException {
		isWritable("Updating node");
		isIdPresentInCollection(String.format("Node with id %s does not exist",
				node.getUri()), node.getNodeGroup().getNodes(), node);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		NodeDao nodeDao = new NodeDao(datasource);
		try {
			nodeDao.update(node);
		} catch (SQLException e) {
			throw new HiveException("Problem updating node: " + e.getMessage());
		}

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
	 * @throws HiveException
	 *             Throws if there is any problem persisting the data, if the
	 *             hive is currently read-only, or if a resource of the same
	 *             name already exists
	 */
	public Resource updateResource(Resource resource) throws HiveException {
		isWritable("Updating resource");
		isIdPresentInCollection(String.format(
				"Resource with id %s does not exist", resource.getId()),
				resource.getPartitionDimension().getResources(), resource);
		isUnique(String.format("Resource with name %s already exists", resource
				.getName()), resource.getPartitionDimension().getResources(),
				resource);

		BasicDataSource datasource = new HiveBasicDataSource(this.getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		try {
			resourceDao.update(resource);
		} catch (SQLException e) {
			throw new HiveException("Problem persisting resource: "
					+ e.getMessage());
		}
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
	 * @throws HiveException
	 *             Throws if there is a problem persisting the data, the hive is
	 *             read-only, or if a secondary index with the same columnInfo()
	 *             name already exists in the resource.
	 */
	public SecondaryIndex updateSecondaryIndex(SecondaryIndex secondaryIndex)
			throws HiveException {
		isWritable("Updating secondary index");
		isIdPresentInCollection(String.format(
				"Secondary index with id %s does not exist", secondaryIndex
						.getId()), secondaryIndex.getResource()
				.getSecondaryIndexes(), secondaryIndex);
		isUnique(String.format("Secondary index with name %s already exists",
				secondaryIndex.getName()), secondaryIndex.getResource()
				.getSecondaryIndexes(), secondaryIndex);

		SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(dataSource);
		try {
			secondaryIndexDao.update(secondaryIndex);
		} catch (SQLException e) {
			throw new HiveException("Problem persisting secondary index: "
					+ e.getMessage());
		}
		incrementAndPersistHive(dataSource);

		sync();
		return secondaryIndex;
	}

	public PartitionDimension deletePartitionDimension(
			PartitionDimension partitionDimension) throws HiveException {
		isWritable(String.format("Deleting partition dimension %s",
				partitionDimension.getName()));
		itemExistsInCollection(
				String
						.format(
								"Partition dimension %s does not match any partition dimension in the hive",
								partitionDimension.getName()),
				getPartitionDimensions(), partitionDimension);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(
				datasource);
		try {
			partitionDimensionDao.delete(partitionDimension);
		} catch (SQLException e) {
			throw new HiveException("Problem deletng the partition dimension",
					e);
		}
		return partitionDimension;
	}

	public Node deleteNode(Node node) throws HiveException {
		isWritable(String.format("Deleting node %s", node.getUri()));
		itemExistsInCollection(
				String
						.format(
								"Node %s does not match any node in the partition dimenesion %s",
								node.getUri(), node.getNodeGroup()
										.getPartitionDimension().getName()),
				node.getNodeGroup().getPartitionDimension().getNodeGroup()
						.getNodes(), node);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		NodeDao nodeDao = new NodeDao(datasource);
		try {
			nodeDao.delete(node);
		} catch (SQLException e) {
			throw new HiveException("Problem deletng the node", e);
		}
		return node;
	}

	public Resource deleteResource(Resource resource) throws HiveException {
		isWritable(String.format("Deleting resource %s", resource.getName()));
		itemExistsInCollection(
				String
						.format(
								"Resource %s does not match any resource in the partition dimenesion %s",
								resource.getName(), resource
										.getPartitionDimension().getName()),
				resource.getPartitionDimension().getResources(), resource);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		ResourceDao resourceDao = new ResourceDao(datasource);
		try {
			resourceDao.delete(resource);
		} catch (SQLException e) {
			throw new HiveException("Problem deletng the resource", e);
		}
		return resource;
	}

	public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex)
			throws HiveException {
		isWritable(String.format("Deleting secondary index %s", secondaryIndex
				.getName()));
		itemExistsInCollection(
				String
						.format(
								"Secondary index %s does not match any node in the resource %s",
								secondaryIndex.getName(), secondaryIndex
										.getResource()), secondaryIndex
						.getResource().getSecondaryIndexes(), secondaryIndex);
		BasicDataSource datasource = new HiveBasicDataSource(getHiveUri());
		SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(datasource);
		try {
			secondaryindexDao.delete(secondaryIndex);
		} catch (SQLException e) {
			throw new HiveException("Problem deletng the secondary index", e);
		}
		return secondaryIndex;
	}

	private void incrementAndPersistHive(DataSource datasource)
			throws HiveException {
		try {
			new HiveSemaphoreDao(datasource).incrementAndPersist();
		} catch (SQLException e) {
			throw new HiveException("Problem incrementing Hive revision: "
					+ e.getMessage());
		}
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
	 * @throws HiveException
	 *             Throws if the partition dimension is not in the hive, or if
	 *             the hive, primary index or node is currently read only.
	 * @throws SQLException
	 *             Throws if the primary index key already exists, or another
	 *             persitence error occurs.
	 */
	public void insertPrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveException, SQLException {
		// TODO Consider redesign of NodeGroup to perform assignment, or at
		// least provider direct iteration over Nodes
		Node node = partitionDimension.getAssigner().chooseNode(
				partitionDimension.getNodeGroup().getNodes(), primaryIndexKey);
		isWritable("Inserting a new primary index key", node);
		getDirectory(partitionDimension).insertPrimaryIndexKey(node,
				primaryIndexKey);
		sync();
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
	 * @throws HiveException
	 *             Throws if the partition dimension is not in the hive, or if
	 *             the hive, primary index or node is currently read only.
	 * @throws SQLException
	 *             Throws if the primary index key already exists, or another
	 *             persitence error occurs.
	 */
	public void insertPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveException, SQLException {
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
	 * @throws HiveException
	 *             Throws if the secondary index does not exist in the hive via
	 *             a resource and partition dimension, or if the hive is
	 *             currently read-only
	 * @throws SQLException
	 *             Throws if the secondary index key already exists in this
	 *             secondary index, or for any other persistence error.
	 */
	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, Object primaryindexKey)
			throws HiveException, SQLException {
		isWritable("Inserting a new secondary index key");
		getDirectory(secondaryIndex.getResource().getPartitionDimension())
				.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKey,
						primaryindexKey);
		statistics.incrementChildRecordCount(secondaryIndex.getResource()
				.getPartitionDimension(), primaryindexKey, 1);
		sync();
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
	 * @throws HiveException
	 *             Throws if the primary index key is not yet in the primary
	 *             index, or if the hive, primary index or node is currently
	 *             read only.
	 * @throws SQLException
	 *             Throws if the secondary index key already exists in this
	 *             secondary index, or for any other persistence error.
	 */
	public void insertSecondaryIndexKey(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey, Object primaryIndexKey)
			throws HiveException, SQLException {
		insertSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				primaryIndexKey);
	}

	/**
	 * 
	 * Updates the node of the given primary index key for the given partition
	 * dimension.
	 * 
	 * @param partitionDimension
	 *            A partition dimension of the hive
	 * @param primaryIndexKey
	 *            An existing primary index key in the primary index
	 * @param node
	 *            A node of the node group of the partition dimension
	 * @throws HiveException
	 *             Throws if the primary index key is not yet in the primary
	 *             index, or if the hive, primary index or node is currently
	 *             read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexNode(PartitionDimension partitionDimension,
			Object primaryIndexKey, Node node) throws HiveException,
			SQLException {
		isWritable("Updating primary index node", partitionDimension.getNodeGroup().getNode(getNodeSemaphoreOfPrimaryIndexKey(
				partitionDimension, primaryIndexKey).getId()), primaryIndexKey,
				getReadOnlyOfPrimaryIndexKey(partitionDimension,
						primaryIndexKey));

		getDirectory(partitionDimension).updatePrimaryIndexKey(node,
				primaryIndexKey);
		sync();
	}

	/**
	 * 
	 * Updates the node of the given primary index key for the given partition
	 * dimension.
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition demension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key in the primary index
	 * @param nodeUri
	 *            A uri of a node of the node group of the partition dimension
	 * @throws HiveException
	 *             Throws if the primary index key is not yet in the primary
	 *             index, or if the hive, primary index or node is currently
	 *             read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexNode(String partitionDimensionName,
			Object primaryIndexKey, String nodeUri) throws HiveException,
			SQLException {
		PartitionDimension partitionDimension = getPartitionDimension(partitionDimensionName);
		updatePrimaryIndexNode(partitionDimension, primaryIndexKey,
				partitionDimension.getNodeGroup().getNode(nodeUri));
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
	 * @throws HiveException
	 *             Throws if the primary index key is not yet in the primary
	 *             index, or if the hive is currently read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexReadOnly(
			PartitionDimension partitionDimension, Object primaryIndexKey,
			boolean isReadOnly) throws HiveException, SQLException {
		isWritable("Updating primary index read-only");
		// This query validates the existence of the primaryIndexKey
		getNodeSemaphoreOfPrimaryIndexKey(partitionDimension, primaryIndexKey);
		getDirectory(partitionDimension).updatePrimaryIndexKeyReadOnly(
				primaryIndexKey, isReadOnly);
		sync();
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
	 * @throws HiveException
	 *             Throws if the primary index key is not yet in the primary
	 *             index, or if the hive is currently read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexReadOnly(String partitionDimensionName,
			Object primaryIndexKey, boolean isReadOnly) throws HiveException,
			SQLException {
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
	 * @throws HiveException
	 *             Throws if the secondary index key is not yet in the secondary
	 *             index, or if the hive is currently read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexKeyOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey,
			Object primaryIndexKey) throws HiveException, SQLException {
		isWritable("Updating primary index key of secondary index key");
		getPrimaryIndexKeyOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		getDirectory(secondaryIndex.getResource().getPartitionDimension())
				.updatePrimaryIndexOfSecondaryKey(secondaryIndex,
						secondaryIndexKey, primaryIndexKey);
		sync();
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
	 * @throws HiveException
	 *             Throws if the secondary index key is not yet in the secondary
	 *             index, or if the hive is currently read only.
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void updatePrimaryIndexKeyOfSecondaryIndexKey(
			String partitionDimensionName, String resourceName,
			String secondaryIndexName, Object secondaryIndexKey,
			Object primaryIndexKey) throws HiveException, SQLException {
		updatePrimaryIndexKeyOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey,
				primaryIndexKey);
	}

	/**
	 * Deletes the primary index key of the given partition dimension
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @throws HiveException
	 *             Throws if the primary index key does not exist or if the
	 *             hive, node of the primary index key, or primary index key
	 *             itself is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deletePrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveException, SQLException {

		if (!doesPrimaryIndexKeyExist(partitionDimension, primaryIndexKey))
			throw new HiveException("The primary index key " + primaryIndexKey
					+ " does not exist");
		isWritable("Deleting primary index key", partitionDimension.getNodeGroup().getNode(getNodeSemaphoreOfPrimaryIndexKey(
				partitionDimension, primaryIndexKey).getId()), primaryIndexKey,
				getReadOnlyOfPrimaryIndexKey(partitionDimension,
						primaryIndexKey));

		for (Resource resource : partitionDimension.getResources())
			for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {
				getDirectory(partitionDimension)
						.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(
								secondaryIndex, primaryIndexKey);
			}

		getDirectory(partitionDimension).deletePrimaryIndexKey(primaryIndexKey);
		sync();
	}

	/**
	 * Deletes a primary index key of the given partition dimension
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @throws HiveException
	 *             Throws if the primary index key does not exist or if the
	 *             hive, node of the primary index key, or primary index key
	 *             itself is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deletePrimaryIndexKey(String partitionDimensionName,
			Object secondaryIndexKey) throws HiveException, SQLException {
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
	 * @throws HiveException
	 *             Throws if the secondary index key does not exist or if the
	 *             hive is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) throws HiveException, SQLException {
		Object primaryIndexKey = getPrimaryIndexKeyOfSecondaryIndexKey(
				secondaryIndex, secondaryIndexKey);
		isWritable("Deleting secondary index key");

		if (!doesSecondaryIndexKeyExist(secondaryIndex, secondaryIndexKey))
			throw new HiveException("Secondary index key "
					+ secondaryIndexKey.toString() + " does not exist");

		getDirectory(secondaryIndex.getResource().getPartitionDimension())
				.deleteSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		statistics.decrementChildRecordCount(secondaryIndex.getResource()
				.getPartitionDimension(), primaryIndexKey, 1);
		sync();
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
	 * @throws HiveException
	 *             Throws if the secondary index key does not exist or if the
	 *             hive is currently read-only
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public void deleteSecondaryIndexKey(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey) throws HiveException, SQLException {
		deleteSecondaryIndexKey(getPartitionDimension(partitionDimensionName)
				.getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey);
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
			PartitionDimension partitionDimension, Object primaryIndexKey)
			throws HiveException, SQLException {
		return getDirectory(partitionDimension).doesPrimaryIndexKeyExist(
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
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public boolean doesPrimaryIndeyKeyExist(String partitionDimensionName,
			Object primaryIndexKey) throws HiveException, SQLException {
		return doesPrimaryIndexKeyExist(
				getPartitionDimension(partitionDimensionName), primaryIndexKey);
	}

	/**
	 * Returns the node assigned to the given primary index key
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            A primary index key belonging to the partition dimension
	 * @return
	 * @throws HiveException
	 *             Throws if the primary index key does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public Node getNodeOfPrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveException, SQLException {
		try {
			return partitionDimension.getNodeGroup().getNode(
					getDirectory(partitionDimension).getNodeSemamphoreOfPrimaryIndexKey(primaryIndexKey).getId());
		} catch (Exception e) {
			throw new HiveException(
					String
							.format(
									"Primary index key %s of partition dimension %s not found.",
									primaryIndexKey.toString(),
									partitionDimension.getName()), e);
		}
	}
	
	public NodeSemaphore getNodeSemaphoreOfPrimaryIndexKey(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveException, SQLException {
		try {
			return getDirectory(partitionDimension).getNodeSemamphoreOfPrimaryIndexKey(primaryIndexKey);
		} catch (Exception e) {
			throw new HiveException(
					String
							.format(
									"Primary index key %s of partition dimension %s not found.",
									primaryIndexKey.toString(),
									partitionDimension.getName()), e);
		}
	}

	/**
	 * Returns the node assigned to the given primary index key
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            A primary index key belonging to the partition dimension
	 * @return
	 * @throws HiveException
	 *             Throws if the partition dimension or primary index key does
	 *             not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public NodeSemaphore getNodeSemaphoreOfPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveException, SQLException {
		return getNodeSemaphoreOfPrimaryIndexKey(
				getPartitionDimension(partitionDimensionName), primaryIndexKey);
	}

	/**
	 * Returns true is the given primary index key is read-only
	 * 
	 * @param partitionDimension
	 *            A partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @return
	 * @throws HiveException
	 *             Throws if the primary index key does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(
			PartitionDimension partitionDimension, Object primaryIndexKey)
			throws HiveException, SQLException {
		Boolean readOnly = getDirectory(partitionDimension)
				.getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
		if (readOnly != null)
			return readOnly;
		throw new HiveException(String.format(
				"Primary index key %s of partition dimension %s not found.",
				primaryIndexKey.toString(), partitionDimension.getName()));
	}

	/**
	 * Returns true is the given primary index key is read-only
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param primaryIndexKey
	 *            An existing primary index key of the partition dimension
	 * @return
	 * @throws HiveException
	 *             Throws if the primary index key does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(String partitionDimensionName,
			Object primaryIndexKey) throws HiveException, SQLException {
		return getReadOnlyOfPrimaryIndexKey(
				getPartitionDimension(partitionDimensionName), primaryIndexKey);
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
	 * @throws SQLException
	 *             Throws an exception if there is a persistence error
	 */
	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) throws SQLException {
		return getDirectory(
				secondaryIndex.getResource().getPartitionDimension())
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
	 * @throws HiveException
	 *             Throws if the partition dimension, resource, or secondary
	 *             index does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public boolean doesSecondaryIndexKeyExist(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey) throws HiveException, SQLException {
		return doesSecondaryIndexKeyExist(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey);
	}

	/**
	 * 
	 * Returns the node of the given secondary index key, based on the node of
	 * the corresponding primary index key
	 * 
	 * @param secondaryIndex
	 *            A secondary index that belongs to the hive via its resource
	 *            and partition dimension
	 * @param secondaryIndexKey
	 *            The secondary index key on which to query
	 * @return
	 * @throws HiveException
	 *             Throws if the secondary index key does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public Node getNodeOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) throws HiveException, SQLException {
		PartitionDimension partitionDimension = secondaryIndex.getResource().getPartitionDimension();
		try {
			return partitionDimension.getNodeGroup().getNode(
					getDirectory(partitionDimension).getNodeSemaphoreOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey).getId());
		} catch (Exception e) {
			throw new HiveException(
					String
							.format(
									"Secondary index key %s of partition dimension %s on secondary index %s not found.",
									secondaryIndex.toString(),
									partitionDimension.getName(),
									secondaryIndex.getName()), e);
		}
	}
	
	public NodeSemaphore getNodeSemaphoreOfSecondaryIndexKey(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) throws HiveException, SQLException {
		PartitionDimension partitionDimension = secondaryIndex.getResource()
				.getPartitionDimension();
		try {
			return getDirectory(partitionDimension).getNodeSemaphoreOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
		} catch (Exception e) {
			throw new HiveException(
					String
							.format(
									"Secondary index key %s of partition dimension %s on secondary index %s not found.",
									secondaryIndex.toString(),
									partitionDimension.getName(),
									secondaryIndex.getName()), e);
		}
	}

	/**
	 * 
	 * Returns the node of the given secondary index key, based on the node of
	 * the corresponding primary index key
	 * 
	 * @param partitionDimensionName
	 *            The name of a partition dimension in the hive
	 * @param resourceName
	 *            The name of a resource in the partition dimesnion
	 * @param secondaryIndexName
	 *            The name of a secondary index of the resource
	 * @param secondaryIndexKey
	 *            The key of the secondary index to test
	 * @return
	 * @throws HiveException
	 *             Throws if the partition dimension, resource, or secondary
	 *             index does not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public NodeSemaphore getNodeSemaphoreOfSecondaryIndexKey(String partitionDimensionName,
			String resourceName, String secondaryIndexName,
			Object secondaryIndexKey) throws HiveException, SQLException {
		return getNodeSemaphoreOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey);
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
	 * @throws HiveException
	 * @throws SQLException
	 */
	public Object getPrimaryIndexKeyOfSecondaryIndexKey(
			SecondaryIndex secondaryIndex, Object secondaryIndexKey)
			throws HiveException, SQLException {
		PartitionDimension partitionDimension = secondaryIndex.getResource()
				.getPartitionDimension();
		Object primaryIndexKey = getDirectory(partitionDimension)
				.getPrimaryIndexKeyOfSecondaryIndexKey(secondaryIndex,
						secondaryIndexKey);
		if (primaryIndexKey != null)
			return primaryIndexKey;
		throw new HiveException(
				String
						.format(
								"Secondary index key %s of partition dimension %s on secondary index %s not found.",
								secondaryIndex.toString(), partitionDimension
										.getName(), secondaryIndex.getName()));
	}

	/**
	 * Returns the primary index key of the given secondary index key
	 * 
	 * @param partitionDimensionName
	 *            A partition dimension of the hive
	 * @param resourceName
	 *            A resource of the partition dimension
	 * @param secondaryIndexName
	 *            A secondary index of the resource
	 * @param secondaryIndexKey
	 *            The secondary index to query for
	 * @return
	 * @throws HiveException
	 *             Throws if the partition dimension, resource, or secondary
	 *             index do not exist
	 * @throws SQLException
	 *             Throws if there is a persistence error
	 */
	public Object getPrimaryIndexKeyOfSecondaryIndexKey(
			String partitionDimensionName, String resourceName,
			String secondaryIndexName, Object secondaryIndexKey)
			throws HiveException, SQLException {
		return getPrimaryIndexKeyOfSecondaryIndexKey(getPartitionDimension(
				partitionDimensionName).getResource(resourceName)
				.getSecondaryIndex(secondaryIndexName), secondaryIndexKey);
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
	 * @throws SQLException
	 *             Throws if there is a persistence error.
	 */
	public Collection getSecondaryIndexKeysWithPrimaryKey(
			SecondaryIndex secondaryIndex, Object primaryIndexKey)
			throws SQLException {
		return getDirectory(
				secondaryIndex.getResource().getPartitionDimension())
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
	 * @throws HiveException
	 * @throws SQLException
	 */
	public Collection getSecondaryIndexKeysWithPrimaryKey(
			String partitionDimensionName, String resource,
			String secondaryIndexName, Object primaryIndexKey)
			throws HiveException, SQLException {
		return getSecondaryIndexKeysWithPrimaryKey(getPartitionDimension(
				partitionDimensionName).getResource(resource)
				.getSecondaryIndex(secondaryIndexName), primaryIndexKey);
	}

	/**
	 * Test if Hive has been installed at current JDBC URI
	 * 
	 * @return True if hive metadata has been installed
	 * @throws HiveException
	 * @throws SQLException
	 */
	public boolean exists() {
		try {
			new HiveSemaphoreDao(dataSource).get();
			return true;
		} catch (Exception ex) {
			return false;
		}
	}
	
	private Connection getConnection(PartitionDimension partitionDimension, NodeSemaphore semaphore, AccessType intention) throws SQLException, HiveException {
		try{
			if( intention == AccessType.ReadWrite && isKeyReadOnly(partitionDimension, semaphore))
				throw new HiveReadOnlyException("The key/node/hive requested cannot be written to at this time.");
			
			Connection conn = DriverManager.getConnection(partitionDimension.getNodeGroup().getNode(semaphore.getId()).getUri());
			if(intention == AccessType.Read)
				conn.setReadOnly(true);
			return conn;
		} catch (SQLException e) {
//			stats.incrementConnectionFailures();
			throw e;
		} catch( RuntimeException e) {
//			stats.incrementConnectionFailures();
			throw e;
		} catch (HiveException e) {
//			stats.incrementConnectionFailures();
			throw e;
		}
	}
	
	private boolean isKeyReadOnly(PartitionDimension partitionDimension, NodeSemaphore semaphore) throws HiveException {
		return isReadOnly() || partitionDimension.getNodeGroup().getNode(semaphore.getId()).isReadOnly() || semaphore.isReadOnly();
	}
	
	public Connection getConnection(PartitionDimension partitionDimension,
			Object primaryIndexKey, AccessType intent) throws HiveException, SQLException {
		return getConnection(partitionDimension, getNodeSemaphoreOfPrimaryIndexKey(partitionDimension, primaryIndexKey), intent);
	}
	
	public Connection getConnection(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey, AccessType intent) throws HiveException, SQLException {
		return getConnection(
				secondaryIndex.getResource().getPartitionDimension(), 
				getNodeSemaphoreOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey), 
				intent);
	}

	public Connection getConnection(PartitionDimension partitionDimension,
			Object primaryIndexKey) throws HiveException, SQLException {
		return getConnection(partitionDimension, primaryIndexKey, AccessType.ReadWrite);
	}

	public Connection getConnection(SecondaryIndex secondaryIndex,
			Object secondaryIndexKey) throws HiveException, SQLException {
		return getConnection(secondaryIndex, secondaryIndexKey, AccessType.ReadWrite);
	}

	// Facade methods that are short cuts over digging into the object graph
	public Collection<String> getNodeUrisForPartitionDimension(
			String partitionDimensionName) throws HiveException {
		Collection<String> collection = new ArrayList<String>();
		for (Node node : getPartitionDimension(partitionDimensionName)
				.getNodeGroup().getNodes())
			collection.add(node.getUri());
		return collection;
	}

	public String toString() {
		return HiveUtils.toDeepFormatedString(this, "HiveUri", getHiveUri(),
				"Revision", getRevision(), "PartitionDimensions",
				getPartitionDimensions());
	}

	private <T extends IdAndNameIdentifiable> void isUnique(
			String errorMessage, Collection<T> collection, T item)
			throws HiveException {
		// Forbids duplicate names for two different instances if the class
		// implements Identifies
		if (!(item instanceof IdAndNameIdentifiable))
			return;
		String itemName = ((IdAndNameIdentifiable) item).getName();
		for (IdAndNameIdentifiable collectionItem : collection)
			if (itemName.equals((collectionItem).getName())
					&& collectionItem.getId() != item.getId())
				throw new HiveException(errorMessage + ", "
						+ collectionItem.getId() + ", " + item.getId());
		return;
	}

	private <T extends Identifiable> void isIdPresentInCollection(
			String errorMessage, Collection<T> collection, T item)
			throws HiveException {
		for (T collectionItem : collection)
			if (item.getId() == collectionItem.getId())
				return;
		throw new HiveException(errorMessage);
	}

	private <T> void itemExistsInCollection(String errorMessage,
			Collection<T> collection, T item) throws HiveException {
		// All classes implement Comparable, so this does a deep compare on all
		// objects owned by the item.
		if (!collection.contains(item))
			throw new HiveException(errorMessage);
	}

	private void isWritable(String errorMessage) throws HiveException {
		if (this.isReadOnly())
			throw new HiveReadOnlyException(
					errorMessage
							+ ". This operation is invalid because the hive is currently read-only.");
	}

	private void isWritable(String errorMessage, Node node)
			throws HiveException {
		isWritable(errorMessage);
		if (node.isReadOnly())
			throw new HiveReadOnlyException(errorMessage
					+ ". This operation is invalid becuase the selected node "
					+ node.getId() + " is currently read-only.");
	}

	private void isWritable(String errorMessage, Node node,
			Object primaryIndexKeyId, boolean primaryIndexKeyReadOnly)
			throws HiveException {
		isWritable(errorMessage, node);
		if (primaryIndexKeyReadOnly)
			throw new HiveReadOnlyException(
					errorMessage
							+ ". This operation is invalid becuase the primary index key "
							+ primaryIndexKeyId.toString()
							+ " is currently read-only.");
	}

	private Directory getDirectory(PartitionDimension dimension) {
		for (Directory directory : directories)
			if (dimension.getName().equals(
					directory.getPartitionDimension().getName()))
				return directory;

		String msg = "Could not find directory for partition dimension "
				+ dimension.getName();
		msg += "Looking for:\n" + dimension.toString();
		msg += "In List: \n";
		for (Directory dir : this.directories)
			msg += dir.getPartitionDimension().toString() + "\n";
		throw new NoSuchElementException(msg);
	}

	public PartitionKeyStatisticsDao getStatistics() {
		return statistics;
	}

}
