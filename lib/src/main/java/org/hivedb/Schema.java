package org.hivedb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.hivedb.meta.TableInfo;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.DriverLoader;
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
	

	
	/**
	 * Boolean types vary from database to database, this method returns the smallest
	 * available type to be used as a boolean.
	 * @param dialect A HiveDbDialect enum
	 * @return
	 */
	public static String getBooleanTypeForDialect(HiveDbDialect dialect)
	{
		if (dialect.equals(HiveDbDialect.MySql))
			return "BOOLEAN";
		if (dialect.equals(HiveDbDialect.Derby))
			return "INT";			
		throw new UnsupportedDialectException("No option boolean option configured for " + dialect.name());
	}
	
	/**
	 * Get the SQL fragment to create an auto-incrementing key sequence for a database dialect.
	 * @param dialect
	 * @return
	 */
	public String getNumericPrimaryKeySequenceModifier(HiveDbDialect dialect) {
		String statement = "";
		if (dialect == HiveDbDialect.MySql)
			statement = "int not null auto_increment primary key";
		else if (dialect == HiveDbDialect.Derby)
			statement = "int not null generated always as identity primary key";
		else 
			throw new UnsupportedDialectException("Unsupported dialect: " + dialect.toString());
		return statement;
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
