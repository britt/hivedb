package org.hivedb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
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
	private String name;
	private Collection<String> uris = new HashSet<String>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	
	public Schema(String name) {
		this.name = name;
	}
	
	protected Context getContext(Node node) {
		return getContext(node.getUri());
	}
	
	protected Context getContext(String uri) {
		Context context = new VelocityContext();
		context.put("dialect", DriverLoader.discernDialect(uri));
		for (HiveDbDialect d : HiveDbDialect.values()) {
			context.put(DialectTools.dialectToString(d).toLowerCase(), d);
		}
		context.put("booleanType", DialectTools.getBooleanTypeForDialect(DriverLoader.discernDialect(uri)));
		context.put("sequenceModifier", DialectTools.getNumericPrimaryKeySequenceModifier(DriverLoader.discernDialect(uri)));
		return context;
	}
	
	/**
	 * Return the SQL statements necessary to create the schema.
	 * 
	 * @return SQL create statements for tables and indexes
	 */
	public abstract Collection<TableInfo> getTables(String uri);
	
	public void install(Node node) {
		install(node.getUri());
	}
	
	public void install(String uri) {
		setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
		for (TableInfo table : getTables(uri)) {
			createTable(table, uri);
		}
		uris.add(uri);
	}
	
	public void emptyTables(Node node) {
		emptyTables(node.getUri());
	}
	
	public void emptyTables(String uri) {
		setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
		for (TableInfo table : getTables(uri)) {
			emptyTable(table, uri);
		}
		uris.remove(uri);
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
	public boolean tableExists(String tableName, String uri)
	{
		setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
		JdbcTemplate t = getJdbcTemplate();
		try {
			t.query( "select * from " + tableName + ifMySql(" LIMIT 1",DriverLoader.discernDialect(uri)), new TrueRowMapper());
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
	private void createTable(TableInfo table, String uri) {
		setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
		JdbcTemplate j = getJdbcTemplate();
		if(!tableExists(table.getName(), uri)) {
			final String createStatement = table.getCreateStatement();
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					createStatement);
			j.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
			/* try {
				System.out.println("Table " + table.getName() + " created for for database " + j.getDataSource().getConnection().toString());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} */
		}
	}
	
	private void emptyTable(TableInfo table, String uri) {
		setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
		JdbcTemplate j = getJdbcTemplate();
		if(tableExists(table.getName(), uri)) {
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
	
	public Collection<String> getURIs() {
		return Collections.unmodifiableCollection(uris);
	}
}
