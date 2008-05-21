package org.hivedb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.Schemas;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;


/**
 * Schema defines a set of tables and/or indexes to be installed in a hive. 
 * Generic DDL is used for portability.
 *
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public abstract class Schema {
	private String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public Schema(String name) {
		this.name = name;
	}
	
	/**
	 * Return the SQL statements necessary to create the schema.
	 * 
	 * @return SQL create statements for tables and indexes
	 */
	public abstract Collection<TableInfo> getTables(String uri);
	
	public static class TrueRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return true;
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Schema) {
			return name.equals(((Schema) obj).getName());
		}
		return false;
	}
}
