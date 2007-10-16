package org.hivedb;

import static org.hivedb.util.database.DialectTools.getNumericPrimaryKeySequenceModifier;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
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
public abstract class Schema extends JdbcDaoSupport {
	protected String dbURI;
	protected HiveDbDialect dialect = HiveDbDialect.MySql;
	
	public Schema(String dbURI){
		this.dbURI = dbURI;
		this.setDataSource(new HiveBasicDataSource(dbURI));
		this.dialect = DriverLoader.discernDialect(dbURI);
	}
	
	protected Context getContext() {
		Context context = new VelocityContext();
		context.put("dialect", dialect);
		context.put("mysql", HiveDbDialect.MySql);
		context.put("derby", HiveDbDialect.Derby);
		context.put("h2", HiveDbDialect.H2);
		context.put("booleanType", DialectTools.getBooleanTypeForDialect(dialect));
		context.put("sequenceModifier", getNumericPrimaryKeySequenceModifier(dialect));
		return context;
	}
	
	/**
	 * Return the SQL statements necessary to create the schema.
	 * 
	 * @return SQL create statements for tables and indexes
	 */
	public abstract Collection<TableInfo> getTables();
	
	/**
	 * Create the schema in the database.
	 */
	public void install() {
		// Register any new Global tables here
		for (TableInfo table : getTables())
			createTable(table);
	}

	public static String addLengthForVarchar(String type)
	{
		if (type.equals("VARCHAR"))
			return "VARCHAR(255)";
		return type;
	}
	
	/**
	 * Test if a table exists by trying to select from it.
	 * @param conn
	 * @param tableName
	 * @return
	 */
	protected boolean tableExists(String tableName)
	{
		JdbcTemplate t = getJdbcTemplate();
		try {
			t.query( "select * from " + tableName + ifMySql(" LIMIT 1", dialect), new TrueRowMapper());
			return true;
		}
		catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * Conditionally create a table using the statement provided if it does
	 * not already exist.
	 * @param conn
	 * @param createStatement
	 * @throws SQLException
	 */
	private void createTable(TableInfo table) {
		JdbcTemplate j = getJdbcTemplate();
		if(!tableExists(table.getName())) {
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					table.getCreateStatement());
			j.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
		}
	}
	
	public static class TrueRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int rowNumber) throws SQLException {
			return true;
		}
	}
	
	public String ifMySql(String sql, HiveDbDialect dialect) {
		return (dialect.equals(HiveDbDialect.MySql) ? sql : "");
	}
}
