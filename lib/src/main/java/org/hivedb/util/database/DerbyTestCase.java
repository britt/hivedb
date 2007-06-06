package org.hivedb.util.database;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class DerbyTestCase extends DatabaseTestCase{
	public static final String TEST_DB = "testDb";
	public Driver driver;
	static {
		try {
			DerbyUtils.getDriver();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the derby driver.", e);
		}
	}
	
	@Override
	@BeforeClass
	protected void beforeClass(){
		try {
			driver = DerbyUtils.getDriver();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the derby driver.", e);
		}
		super.beforeClass();
	}
	
	@Override
	@AfterClass
	protected void afterClass() {
		super.afterClass();
		try {
			DriverManager.deregisterDriver(driver);
		} catch (SQLException e) {
			//quash
		}
	}
	
	@Override
	protected void createDatabase(String name) {
		try {
			DerbyUtils.createDatabase(name, userName, password);
		} catch (Exception e) {
			throw new RuntimeException("Error initializing the Derby database: " + e.getMessage(), e);
		}
	}

	@Override
	protected boolean databaseExists(String name) {
		String path = null;
		try {
			path = new File(".").getCanonicalPath() + File.separator + name;
		} catch (IOException e) {
			throw new RuntimeException("Could not verify the existence of " + name ,e);
		}
		File db = new File(path);
		return db.exists();
	}

	@Override
	protected void deleteDatabase(String name) {
		if(databaseExists(name))
			try {
				DerbyUtils.shutdown(name);
				DerbyUtils.deleteDatabase(new File(".").getCanonicalPath(), name);
			} catch (IOException e) {
				throw new RuntimeException("Error occurred while deleting database " + name, e);
			}
	}

	@Override
	protected String getConnectString(String name) {
		return DerbyUtils.connectString(name);
	}

	@Override
	protected Connection getConnection(String name) {
		Connection connection = null;
		try {
			connection = DerbyUtils.getConnection(name, userName, password);
		} catch (Exception e) {
			throw new RuntimeException("Error connecting to " + name,e);
		}
		return connection;
	}

	@Override
	protected DataSource getDataSource(String name) {
		return new HiveBasicDataSource(getConnectString(name));
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[] {TEST_DB});
	}
}
