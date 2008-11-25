/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import org.hivedb.persistence.ColumnInfo;
import org.hivedb.util.HiveUtils;

/**
 * An index of a value of a resource.
 *
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class SecondaryIndexImpl implements Comparable<SecondaryIndex>, SecondaryIndex {
  private int id;
  // TODO break circular dependencies
  private Resource resource;
  private ColumnInfo columnInfo;


  public SecondaryIndexImpl(String name, int type) {
    this(Hive.NEW_OBJECT_ID, name, type);
  }

  public SecondaryIndexImpl(int id, String name, int type) {
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
  
  public boolean equals(Object obj) {
    return obj.hashCode() == hashCode();
  }

  public int hashCode() {
    return HiveUtils.makeHashCode(new Object[]{
        id,columnInfo
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
    return new SecondaryIndexImpl(getColumnInfo().getName(), getColumnInfo().getColumnType());
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
