package org.hivedb.util.database.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;

import javax.sql.DataSource;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.QuickCache;

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
	
	protected String getHiveDatabaseName() {
		return "hive";
	}
	QuickCache quickCache = new QuickCache();
	protected Hive getOrCreateHive(final String dimensionName) {
		return quickCache.get(dimensionName, new Delay<Hive>() {
			public Hive f() {
				return  Hive.create(
						getConnectString(getHiveDatabaseName()),
						dimensionName,
						Types.INTEGER);
			}
		});
	}
}
