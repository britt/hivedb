/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.persistence.ColumnInfo;
import org.hivedb.util.HiveUtils;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An index entity in the Hive.
 *
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class ResourceImpl implements Comparable<Resource>, Resource {
  private int id;
  private Collection<SecondaryIndex> secondaryIndexes = null;
  private ColumnInfo columnInfo;
  private ResourceIndex idIndex;
  private boolean isPartitioningResource;

  public ResourceImpl() {
  }

  /**
   * Create Constructor
   *
   * @param name
   */
  public ResourceImpl(String name, int columnType, boolean isPartitioningResource) {
    this(Hive.NEW_OBJECT_ID, name, columnType, isPartitioningResource, new ArrayList<SecondaryIndex>());
  }

  /**
   * Create Constructor
   *
   * @param name
   * @param secondaryIndexes
   */
  public ResourceImpl(String name, int columnType, boolean isPartitioningResource, Collection<SecondaryIndex> secondaryIndexes) {
    this(Hive.NEW_OBJECT_ID, name, columnType, isPartitioningResource, secondaryIndexes);
  }

  /**
   * PERSISTENCE LOAD ONLY --
   * The reference to PartitionDimension will be set by the PartitionDimension constructor.
   *
   * @param id
   * @param name
   * @param secondaryIndexes
   */
  public ResourceImpl(int id, String name, int columnType, boolean isPartitioningResource, Collection<SecondaryIndex> secondaryIndexes) {
    this.id = id;
    this.columnInfo = new ColumnInfo(name, columnType);
    this.isPartitioningResource = isPartitioningResource;
    this.secondaryIndexes = insetThisInstance(secondaryIndexes);
    this.idIndex = new ResourceIndex(name, columnType, this);
  }

  private Collection<SecondaryIndex> insetThisInstance(Collection<SecondaryIndex> secondaryIndexes) {
    for (SecondaryIndex secondaryindex : secondaryIndexes)
      secondaryindex.setResource(this);
    return secondaryIndexes;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return columnInfo.getName();
  }

  public void setName(String name) {
    this.columnInfo = new ColumnInfo(name, columnInfo.getColumnType());
  }

  public boolean isPartitioningResource() {
    return isPartitioningResource;
  }

  public void setPartitioningResource(boolean value) {
    isPartitioningResource = value;
  }

  public Collection<SecondaryIndex> getSecondaryIndexes() {
    return secondaryIndexes;
  }

  public SecondaryIndex getSecondaryIndex(String secondaryIndexName) {
    for (SecondaryIndex secondaryIndex : getSecondaryIndexes())
      if (secondaryIndex.getName().equalsIgnoreCase(secondaryIndexName))
        return secondaryIndex;
    throw new HiveRuntimeException(String.format("Secondary index %s of resource %s of partitition dimension %s not found.",
      secondaryIndexName,
      getName()));
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
      getName(), HiveUtils.makeHashCode(secondaryIndexes)
    });
  }

  public String toString() {
    return HiveUtils.toDeepFormatedString(this,
      "Id", getId(),
      "Name", getName());
  }

  public int compareTo(Resource o) {
    return getName().compareTo(o.getName());
  }

  public Object clone() {
    return new ResourceImpl(columnInfo.getName(), columnInfo.getColumnType(), isPartitioningResource, secondaryIndexes);
  }


  public ResourceIndex getIdIndex() {
    return idIndex;
  }

  public int getColumnType() {
    return getIdIndex().getColumnInfo().getColumnType();
  }

  public void setSecondaryIndexes(Collection<SecondaryIndex> secondaryIndexes) {
    this.secondaryIndexes = secondaryIndexes;
  }

  public void setId(Integer field) {
    this.id = field;
  }
}
