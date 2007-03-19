/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import javax.sql.DataSource;

import org.hivedb.management.KeyAuthority;
import org.hivedb.management.MySqlKeyAuthority;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class Mysql implements KeyAuthorityCreator {
	
	private String connectStringWithoutDatabase, connectStringWithDatabase, databaseName;
	private DataSource datasource;
	public Mysql(String connectStringWithoutDatabase, String connectStringWithDatabase, String databaseName, DataSource datasource)
	{
		this.connectStringWithoutDatabase = connectStringWithoutDatabase;
		this.connectStringWithDatabase = connectStringWithDatabase;
		this.databaseName = databaseName;
		this.datasource = datasource;
	}
	@SuppressWarnings("unchecked")
	public <T extends Number> KeyAuthority<T> create(Class keySpace, Class<T> returnType) {
		return new MySqlKeyAuthority<T>(datasource, keySpace ,returnType);
	}
	
	public DataSource getDataSource() {
		DataSource datasource = new HiveBasicDataSource(getConnectStringWithoutDatabase());
		try {
			datasource.getConnection().createStatement().execute(
					"DROP DATABASE " + getDatabaseName());
		} catch (Exception ex) { // ok
		}
		try {
			datasource.getConnection().createStatement().execute(
				"CREATE DATABASE " + getDatabaseName());
			datasource = new HiveBasicDataSource(getConnectString());
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		return datasource;
	}
	
	private String getConnectStringWithoutDatabase() {
		return connectStringWithoutDatabase;
	}

	private String getConnectString() {
		return connectStringWithDatabase;
	}

	private String getDatabaseName() {
		return databaseName;
	}
}