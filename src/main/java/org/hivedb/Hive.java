/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.configuration.persistence.*;
import org.hivedb.util.Preconditions;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;

import javax.sql.DataSource;
import java.util.Collection;

/**
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 *         <p/>
 *         The facade for all fundamental CRUD operations on the Hive directories and Hive metadata.
 */
public class Hive {
  public static final int NEW_OBJECT_ID = 0;

  public HiveConfiguration hiveConfiguration;
  private ConnectionManager connection;
  private DirectoryFacade directory;
  private DataSource hiveDataSource;

  public Hive(HiveConfiguration hiveConfiguration, DirectoryFacade directory, ConnectionManager connection, DataSource hiveDataSource) {
    this.connection = connection;
    this.directory = directory;
    this.hiveConfiguration = hiveConfiguration;
    this.hiveDataSource = hiveDataSource;
  }

  public void updateHiveStatus(Lockable.Status status) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public int getRevision() {
    return getSemaphore().getRevision();
  }

  public HiveSemaphore getSemaphore() {
    return hiveConfiguration.getSemaphore();
  }

  public PartitionDimension getPartitionDimension() {
    return hiveConfiguration.getPartitionDimension();
  }

  private void incrementAndPersistHive(DataSource datasource) {
    new HiveSemaphoreDao(datasource).incrementAndPersist();
  }

  public ConnectionManager connection() {
    return this.connection;
  }

  public String toString() {
    return hiveConfiguration.toString();
  }

  public DirectoryFacade directory() {
    return this.directory;
  }

  public void updateNodeStatus(NodeImpl node, Lockable.Status status) {
    node.setStatus(status);
    try {
      this.updateNode(node);
    } catch (HiveLockableException e) {
      //quash since the hive is already read-only
    }
  }

  public Node addNode(Node node)
    throws HiveLockableException {

    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.nameIsUnique(getNodes(), node);

    NodeDao nodeDao = new NodeDao(hiveDataSource);
    nodeDao.create(node);

    incrementAndPersistHive(hiveDataSource);
    return node;
  }

  public Collection<NodeImpl> addNodes(Collection<NodeImpl> nodes) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());

    for (NodeImpl node : nodes) {
      Preconditions.nameIsUnique(getNodes(), node);
      NodeDao nodeDao = new NodeDao(hiveDataSource);
      nodeDao.create(node);
    }

    incrementAndPersistHive(hiveDataSource);
    return nodes;
  }

  public Resource addResource(Resource resource) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.isNameUnique(getPartitionDimension().getResources(), resource.getName());

    ResourceDao resourceDao = new ResourceDao(hiveDataSource);
    resourceDao.create(resource);
    incrementAndPersistHive(hiveDataSource);
    Schemas.install(getPartitionDimension());
    return getPartitionDimension().getResource(resource.getName());
  }

  public SecondaryIndex addSecondaryIndex(Resource resource, SecondaryIndex secondaryIndex) throws HiveLockableException {
    secondaryIndex.setResource(resource);

    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.nameIsUnique(resource.getSecondaryIndexes(), secondaryIndex);

    SecondaryIndexDao secondaryIndexDao = new SecondaryIndexDao(hiveDataSource);
    secondaryIndexDao.create(secondaryIndex);
    incrementAndPersistHive(hiveDataSource);
    Schemas.install(getPartitionDimension());
    return secondaryIndex;
  }

  /**
   * Updates the Hive's PartitionDimension to the given instance. The hive partition_dimension_metadata
   * table is updated immediately to reflect any changes. Note that changes to nodes are not
   * updated by this call and must be updated explicitly. See {@see #updateNod(Node)}.
   * This method increments the hive version.
   *
   * @param partitionDimension
   * @return the given PartitionDimension
   * @throws HiveLockableException
   */
  public PartitionDimension updatePartitionDimension(PartitionDimension partitionDimension) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());

    PartitionDimensionDao partitionDimensionDao = new PartitionDimensionDao(hiveDataSource);
    partitionDimensionDao.update(partitionDimension);
    incrementAndPersistHive(hiveDataSource);

    return partitionDimension;
  }

  /**
   * Updates the Node of the hive matching the id of the given Node to the given instance.
   * The hive node_metadata table is updated immediately to reflect any changes, and the hive version is incremented.
   *
   * @return the given PartitionDimension
   * @throws HiveLockableException
   */
  public Node updateNode(NodeImpl node) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.idIsPresentInList(getNodes(), node);

    new NodeDao(hiveDataSource).update(node);
    incrementAndPersistHive(hiveDataSource);
    return node;
  }

  public Node deleteNode(NodeImpl node) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.idIsPresentInList(getNodes(), node);

    NodeDao nodeDao = new NodeDao(hiveDataSource);
    nodeDao.delete(node);
    incrementAndPersistHive(hiveDataSource);
    //Synchronize the DataSourceCache
    connection.removeNode(node);
    return node;
  }

  public Resource deleteResource(Resource resource) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.idIsPresentInList(this.getPartitionDimension().getResources(), resource);

    ResourceDao resourceDao = new ResourceDao(hiveDataSource);
    resourceDao.delete(resource);
    incrementAndPersistHive(hiveDataSource);

    return resource;
  }

  public SecondaryIndex deleteSecondaryIndex(SecondaryIndex secondaryIndex) throws HiveLockableException {
    Preconditions.isWritable(hiveConfiguration.getSemaphore());
    Preconditions.idIsPresentInList(secondaryIndex.getResource().getSecondaryIndexes(), secondaryIndex);

    SecondaryIndexDao secondaryindexDao = new SecondaryIndexDao(hiveDataSource);
    secondaryindexDao.delete(secondaryIndex);
    incrementAndPersistHive(hiveDataSource);

    return secondaryIndex;
  }

  public Collection<Node> getNodes() {
    return hiveConfiguration.getNodes();
  }

  public Node getNode(final String name) {
    return Filter.grepSingle(new Predicate<Node>() {
      public boolean f(Node item) {
        return item.getName().equalsIgnoreCase(name);
      }
    }, getNodes());
  }

  /**
   * Returns the node with the given id
   *
   * @param id
   * @return
   */
  public Node getNode(final int id) {
    return Filter.grepSingle(new Predicate<Node>() {
      public boolean f(Node item) {
        return item.getId() == id;
      }
    }, getNodes());
  }

  public HiveConfiguration getHiveConfiguration() {
    return hiveConfiguration;
  }
}
