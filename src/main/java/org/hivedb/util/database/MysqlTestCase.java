package org.hivedb.util.database;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.sql.DataSource;

import org.hivedb.meta.persistence.HiveBasicDataSource;

public class MysqlTestCase extends DatabaseTestCase {
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
		}
	}

	@Override
	protected boolean databaseExists(String name) {
		try{
			getConnection(name);
			return true;
		} catch(Exception e) {
			return false;
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
		return new HiveBasicDataSource(getConnectString(name));
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
