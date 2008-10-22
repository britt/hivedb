/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.Hive;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.Preconditions;
import org.hivedb.util.database.JdbcTypeMapper;

import java.util.ArrayList;
import java.util.Collection;

/**
 * PartitionDimension is the value we use to distribute records to data nodes.  It is
 * the central node of a related graph of secondary indexes, groups, and nodes.
 *
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class PartitionDimensionImpl implements Comparable<PartitionDimension>, Cloneable, IdAndNameIdentifiable<Integer>, PartitionDimension {
  private int id;
  private String name;
  private int columnType;
  private String indexUri;
  private Collection<Resource> resources;

  /**
   * Create constructor. This version does not require an index URI. The index
   * URI will be inherited from the hive so that the indexes are stored at the same
   * URI as the hive metadata. This should be the typical configuration.
   *
   * @param name
   * @param columnType
   * @param nodes
   * @param resources
   */
  public PartitionDimensionImpl(String name, int columnType, Collection<Resource> resources) {
    this(Hive.NEW_OBJECT_ID, name, columnType, null,
      resources);
  }

  /**
   * Create constructor. Primarily used for interactively constructing a new Partition Dimension.
   * <p/>
   * Constructs an empty NodeGroup and Resource collection.
   *
   * @param name
   * @param columnType
   */
  public PartitionDimensionImpl(String name, int columnType) {
    this(name, columnType, new ArrayList<Resource>());
  }

  /**
   * PERSISTENCE LOAD ONLY-- load a PartitionDimension from persistence.
   *
   * @param id
   * @param name
   * @param columnType
   * @param nodes
   * @param indexUri
   * @param resources
   */
  public PartitionDimensionImpl(int id, String name, int columnType, String indexUri,
                                Collection<Resource> resources) {
    super();
    this.id = id;
    this.name = name;
    this.columnType = columnType;
    this.indexUri = indexUri;
    this.resources = insetResources(resources);
  }

  private Collection<Resource> insetResources(
    Collection<Resource> resources) {
    for (Resource resource : resources)
      resource.setPartitionDimension(this);
    return resources;
  }

  public int getColumnType() {
    return columnType;
  }

  public void setColumnType(int columnType) {
    this.columnType = columnType;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getIndexUri() {
    return indexUri;
  }

  public void setIndexUri(String indexUri) {
    this.indexUri = indexUri;
  }

  public Collection<Resource> getResources() {
    return resources;
  }

  public Resource getResource(String resourceName) {
    for (Resource resource : resources)
      if (resource.getName().equalsIgnoreCase(resourceName))
        return resource;
    throw new HiveKeyNotFoundException("Resource with name " + resourceName + " not found.", resourceName);
  }

  /**
   * For use by persistence layer and unit tests.  Otherwise, id should be considered immmutable.
   *
   * @param id Database-generated identifier with which this instance should be updated
   */
  public void updateId(int id) {
    this.id = id;
  }

  public boolean equals(Object obj) {
    return obj.hashCode() == hashCode();
  }

  public int hashCode() {
    return HiveUtils.makeHashCode(new Object[]{
      name, columnType, indexUri, resources
    });
  }

  public String toString() {
    String columnType = null;
    try {
      columnType = JdbcTypeMapper.jdbcTypeToString(getColumnType());
    }
    catch (Exception e) {
      columnType = "Error resolving column type: " + e.getMessage();
    }
    return HiveUtils.toDeepFormatedString(this,
      "Id", getId(),
      "Name", getName(),
      "IndexUri", getIndexUri(),
      "ColumnType", columnType,
      "Resources", getResources());
  }

  public int compareTo(PartitionDimension o) {
    return getName().compareTo(o.getName());
  }

  public Object clone() {
    return new PartitionDimensionImpl(name, columnType);
  }

  public void setId(Integer id) {
    this.id = id;
  }

  /**
   * {@inheritDoc}
   *
   * @param resourceName
   */
  public boolean doesResourceExist(String resourceName) {
    return !Preconditions.isNameUnique(getResources(), resourceName);
  }
}
