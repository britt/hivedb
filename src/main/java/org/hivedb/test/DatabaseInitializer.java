package org.hivedb.test;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import org.hivedb.util.database.HiveDbDialect;

public interface DatabaseInitializer {

	public List<String> getDatabaseNames();
	public void setDatabaseNames(List<String> names);
	
	public void createDatabase(String name);

	public boolean databaseExists(String name);

	public void deleteDatabase(final String name);

	public String getConnectString(String name);

	public Connection getConnection(String name);

	public DataSource getDataSource(String name);

	public HiveDbDialect getDialect();
	
	public String getHost();
}