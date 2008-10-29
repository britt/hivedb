/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.Hive;
import org.hivedb.persistence.ColumnInfo;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.IdAndNameIdentifiable;

/**
 * An index of a value of a resource.
 *
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class SecondaryIndex implements Comparable<SecondaryIndex>, IdAndNameIdentifiable<Integer> {
  private int id;
  // TODO break circular dependencies
  private Resource resource;
  private ColumnInfo columnInfo;

  /**
   * Create constructor
   */
  public SecondaryIndex(String name, int type) {
    this(Hive.NEW_OBJECT_ID, name, type);
  }

  /**
   * PERSISTENCE LOAD ONLY
   * Reference to Resource will be loaded by the Resource constructor
   *
   * @param id
   */
  public SecondaryIndex(int id, String name, int type) {
    this.id = id;
    this.columnInfo = new ColumnInfo(name, type);
  }

  public Integer getId() {
    return id;
  }

  /**
   * A secondary index's name is the compound form resource_name.column_name
   *
   * @return
   */
  public String getName() {
    return this.getColumnInfo().getName();
  }

  public String getTableName() {
    return getTableName(this, this.getResource());
  }

  public ColumnInfo getColumnInfo() {
    return columnInfo;
  }

  public void setColumnInfo(ColumnInfo columnInfo) {
    this.columnInfo = columnInfo;
  }

  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
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
        columnInfo
    });
  }

  public String toString() {
    return HiveUtils.toDeepFormatedString(this,
        "ColumnInfo", getColumnInfo());
  }

  public int compareTo(SecondaryIndex o) {
    return getName().compareTo(o.getName());
  }

  public Object clone() {
    return new SecondaryIndex(getColumnInfo().getName(), getColumnInfo().getColumnType());
  }

  public static String getTableName(SecondaryIndex secondaryIndex, Resource resource) {
    return getTableName(secondaryIndex.getColumnInfo().getName(), resource.getName());
  }

  public static String getTableName(String index, String resource) {
    return resource + "." + index;
  }

  public void setId(Integer field) {
    this.id = field;
	}
}
