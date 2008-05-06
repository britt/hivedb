package org.hivedb.util.database.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import javax.sql.DataSource;

import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class MysqlTestCase extends DatabaseTestCase {
	
	protected String userName = "test";
	protected String password = "test";
	
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Collection<Schema> getSchemas() {
		return Collections.emptyList();
	}
	
	@Override
	protected void createDatabase(String name) {
		Connection connection = getControlConnection();
		try {
			connection.prepareStatement("create database " + name)
					.execute();
			connection.close();
		} catch (Exception e) {
			throw new RuntimeException("Unable to create database "
					+ name, e);
		} finally {
			closeConnection(name, connection);
		}
	}

	@Override
	protected boolean databaseExists(String name) {
		Connection connection = null;
		try{
			connection = getConnection(name);
			return true;
		} catch(Exception e) {
			return false;
		} finally {
			if(connection != null)
				closeConnection(name, connection);
		}
	}

	private void closeConnection(String name, Connection connection) {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new HiveRuntimeException("Failed to close connection to " + name);
		}
	}

	@Override
	protected void deleteDatabase(String name) {
		Connection connection = getControlConnection();
		try {
			connection.prepareStatement("drop database " + name).execute();
		} catch (Exception e) {
			throw new RuntimeException("Unable to drop database "
					+ name, e);
		} finally {
			closeConnection(name, connection);
		}
	}

	

	@Override
	protected String getConnectString(String name) {
		return String.format("jdbc:mysql://localhost/%s?user=%s&password=%s", name, this.userName, this.password);
	}

	@Override
	protected Connection getConnection(String name) {
		return getMysqlConnection(getConnectString(name));
	}

	@Override
	protected DataSource getDataSource(String name) {
		return CachingDataSourceProvider.getInstance().getDataSource(getConnectString(name));
	}

	protected String getDatabaseAgnosticConnectString() {
		return String.format("jdbc:mysql://localhost/?user=%s&password=%s", this.userName, this.password);
	}
	
	private Connection getMysqlConnection(String uri) {
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(uri);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return connection;
	}
	
	private Connection getControlConnection() {
		return getMysqlConnection(getDatabaseAgnosticConnectString());
	}
}
