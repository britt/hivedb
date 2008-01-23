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
	protected String uri;
	protected HiveDbDialect dialect = HiveDbDialect.MySql;
	private String name;	
	
	public String getUri() {
		return uri;
	}

	public void setUri(String dbURI) {
		this.uri = dbURI;
		this.setDataSource(new HiveBasicDataSource(dbURI));
		this.dialect = DriverLoader.discernDialect(dbURI);
	}

	public HiveDbDialect getDialect() {
		return dialect;
	}

	public void setDialect(HiveDbDialect dialect) {
		this.dialect = dialect;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Schema(String name) {this.name = name;}
	
	public Schema(String name, String dbURI){
		this(name);
		setUri(dbURI);
	}

	protected Context getContext() {
		Context context = new VelocityContext();
		context.put("dialect", dialect);
		for(HiveDbDialect d : HiveDbDialect.values())
			context.put(DialectTools.dialectToString(d).toLowerCase(), d);
		context.put("booleanType", DialectTools.getBooleanTypeForDialect(dialect));
		context.put("sequenceModifier", DialectTools.getNumericPrimaryKeySequenceModifier(dialect));
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
		for (TableInfo table : getTables())
			createTable(table);
	}
	
	public void install(String uri){
		this.uri = uri;
		this.setDataSource(new HiveBasicDataSource(uri));
		this.dialect = DriverLoader.discernDialect(uri);
		install();
	}
	
	public void emptyTables(String uri) {
		this.uri = uri;
		this.setDataSource(new HiveBasicDataSource(uri));
		this.dialect = DriverLoader.discernDialect(uri);
		for (TableInfo table : getTables())
			emptyTable(table);
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
	public boolean tableExists(String tableName)
	{
		JdbcTemplate t = getJdbcTemplate();
		try {
			t.query( "select * from " + tableName + ifMySql(" LIMIT 1", dialect), new TrueRowMapper());
			//System.err.println("Table " + tableName + " exists for database " + dbURI);
			return true;
		}
		catch (Exception e) {
			//System.err.println("Table " + tableName + " does not for database " + dbURI);
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
			final String createStatement = table.getCreateStatement();
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					createStatement);
			j.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
			//System.err.println("Table " + table.getName() + " created for for database " + dbURI);
		}
	}
	
	private void emptyTable(TableInfo table) {
		JdbcTemplate j = getJdbcTemplate();
		if(tableExists(table.getName())) {
			final String createStatement = table.getDeleteAllStatement();
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					createStatement);
			j.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
			//System.err.println("Table " + table.getName() + " created for for database " + dbURI);
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
