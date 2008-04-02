package org.hivedb;

import java.util.Collection;
import java.util.Observer;

import org.hivedb.Lockable.Status;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.directory.DirectoryFacade;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.HiveDbDialect;

public interface HiveFacade {

	/**
	 * Retrieve the URI of the hive index database.
	 */
	public String getUri();

	/**
	 * Get the DataSourceProvider used by the Hive to resolve the DataSource of a data node.
	 */
	public DataSourceProvider getDataSourceProvider();

	/**
	 * 
	 * @return
	 */
	public Status getStatus();

	public int getRevision();

	public HiveSemaphore getSemaphore();

	public PartitionDimension getPartitionDimension();

	public HiveDbDialect getDialect();

	public Assigner getAssigner();
	/**
	 *  Adds a new data node to the hive, incrementing the Hive version.
	 */
	public Node addNode(Node node) throws HiveLockableException;

	/**
	 *  Adds a collection of new nodes the hive, incrementing the Hive version.
	 */
	public Collection<Node> addNodes(Collection<Node> nodes)
			throws HiveLockableException;

	boolean doesResourceExist(String resourceName);
	public Resource addResource(Resource resource)
			throws HiveLockableException;

	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveLockableException;
	/**
	 *  Removes the Hive Node matching the id of the given instance. The hive node_metadata table is updated
	 *  immediately to reflect any changes. This method increments the hive version. 
	 */
	public Node deleteNode(Node node) throws HiveLockableException;

	/**
	 *  Removes the Hive Resource matching the id of the given instance. The hive resource_metadata table is updated
	 *  immediately to reflect any changes. This method increments the hive version. 
	 */
	public Resource deleteResource(Resource resource)
			throws HiveLockableException;

	/**
	 *  Removes the Hive SecondaryIndex matching the id of the given instance. The hive secondary_index_metadata table is updated
	 *  immediately to reflect any changes. This method increments the hive version. 
	 */
	public SecondaryIndex deleteSecondaryIndex(
			SecondaryIndex secondaryIndex) throws HiveLockableException;

	public Collection<Node> getNodes();

	public DirectoryFacade directory();

	public Node getNode(String nodeName);

	public void addObserver(Observer observer);
}