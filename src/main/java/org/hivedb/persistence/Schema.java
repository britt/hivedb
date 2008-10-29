package org.hivedb.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.hivedb.persistence.TableInfo;
import org.springframework.jdbc.core.RowMapper;


/**
 * Schema defines a set of tables and/or indexes to be installed in a hive. 
 * Generic DDL is used for portability.
 *
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public abstract class Schema {
	private String name;
	
	/**
	 * Retrieve the schema name
	 * @return The schema name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Set the schema name
	 * @param name The schema name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @param name The schema name
	 */
	public Schema(String name) {
		this.name = name;
	}
	
	/**
	 * Return the SQL statements necessary to create the schema.
	 * 
	 * @return SQL create statements for tables and indexes
	 */
	public abstract Collection<TableInfo> getTables(String uri);
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Schema) {
			return name.equals(((Schema) obj).getName());
		}
		return false;
	}
	
	public static class TrueRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return true;
		}
	}
}
