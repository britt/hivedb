/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;

import org.hivedb.Hive;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.functional.Atom;

/**
 * 
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 * @author Andy Likuski (alikuski@cafepress.com)
 */
public class SecondaryIndex implements Comparable<SecondaryIndex>, IdAndNameIdentifiable {
	private int id;
	private Resource resource;
	private ColumnInfo columnInfo;
	
	/**
	 * 
	 * Create constructor
	 * 
	 * @param columnInfo
	 */
	public SecondaryIndex(ColumnInfo columnInfo) {
		this(Hive.NEW_OBJECT_ID, columnInfo);
	}
	
	/**
	 * 
	 * PERSISTENCE LOAD ONLY
	 * Reference to Resource will be loaded by the Resource constructor
	 * 
	 * @param id
	 * @param columnInfo
	 */
	public SecondaryIndex(int id, ColumnInfo columnInfo) {
		super();
		this.id = id;
		this.columnInfo = columnInfo;
	}

	public int getId() {
		return id;
	}
	
	/**
	 * 
	 *  A secondary index's name is the compound form resource_name.column_name
	 *  
	 * @return
	 */
	public String getName() {
		return SecondaryIndex.getSecondaryIndexName(this, getResource());
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
	
	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
			columnInfo
		});
	}
	
	public String toString()
	{
		return HiveUtils.toDeepFormatedString(this, 
										"ColumnInfo", getColumnInfo());			
	}

	public int compareTo(SecondaryIndex o) {
		return getName().compareTo(o.getName());
	}
	
	public Object clone()
	{
		return new SecondaryIndex(columnInfo);
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public static String getResourceName(String secondaryIndexName) {
		return Atom.getFirstOrThrow(secondaryIndexName.split("\\."));
	}
	
	public static String getSecondaryIndexName(SecondaryIndex secondaryIndex, Resource resource) {
		return getSecondaryIndexName(secondaryIndex.getColumnInfo().getName(), resource.getName());
	}
	
	public static String getSecondaryIndexName(String index, String resource) {
		return resource + "." + index;
	}
}
