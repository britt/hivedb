package org.hivedb.util.database.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import javax.sql.DataSource;

import org.h2.tools.DeleteDbFiles;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class H2TestCase extends DatabaseTestCase {
	public static final String TEST_DB = "testDb";
	
	static {
		try {
			Class.forName("org.h2.Driver").newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the h2 server.", e);
		}
	}
	
	@Override
	protected void createDatabase(String name) {
		try {
			getConnection(name).close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected boolean databaseExists(String name) {
		return true;
	}

	@Override
	protected void deleteDatabase(final String name) {
		try {
			getConnection(name).createStatement().execute("SHUTDOWN");
			DeleteDbFiles.execute("./", name, true);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected String getConnectString(String name) {
		return String.format("jdbc:h2:mem:%s;LOCK_MODE=3", name);
	}

	@Override
	protected Connection getConnection(String name) {
			try {
				return DriverManager.getConnection(getConnectString(name)); 
			} catch (Exception e) {
				throw new RuntimeException("Error connecting to " + name,e);
			}
	}
	
	@Override
	protected DataSource getDataSource(String name) {
		return new HiveBasicDataSource(getConnectString(name));
	}
}
