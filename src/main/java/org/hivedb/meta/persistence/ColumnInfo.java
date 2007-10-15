/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import org.hivedb.util.HiveUtils;
import org.hivedb.util.database.JdbcTypeMapper;

/**
 * 
 * @author Andy Likuski (alikuski@cafepress.com)
 *
 */
public class ColumnInfo {
	private String name;
	private int columnType;
	
	public ColumnInfo(String name, int columnType) {
		super();
		this.name = name;
		this.columnType = columnType;
	}

	public int getColumnType() {
		return columnType;
	}

	public String getName() {
		return name;
	}
	
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {
				name, columnType
		});
	}
	
	public String toString()
	{
		String columnType = null;
		try {
			columnType = JdbcTypeMapper.jdbcTypeToString(getColumnType());
		}
		catch (Exception e)
		{
			columnType = "Error resolving column type: " + e.getMessage();
		}
		return HiveUtils.toDeepFormatedString(this, 
											"Name", 		getName(), 
											"ColumnType", 	columnType);
	}
}
