package org.hivedb.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.h2.tools.DeleteDbFiles;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.HiveDbDialect;

public class H2ContextInitializer implements TestContextInitializer, DatabaseInitializer{
	private List<String> names;
	
	public H2ContextInitializer(){}
	
	public H2ContextInitializer(List<String> names) {
		this.names = names;
	}
	
	public H2ContextInitializer(String...names) {
		this.names = Arrays.asList(names);
	}
	
	public List<String> getDatabaseNames() {return names;}
	
	static {
		try {
			Class.forName("org.h2.Driver").newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the h2 server.", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#createDatabase(java.lang.String)
	 */
	public void createDatabase(String name) {
		try {
			getConnection(name).close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#databaseExists(java.lang.String)
	 */
	public boolean databaseExists(String name) {
		try {
			getConnection(name);
			return true;
		} catch(Exception e) {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#deleteDatabase(java.lang.String)
	 */
	public void deleteDatabase(final String name) {
		try {
			getConnection(name).createStatement().execute("SHUTDOWN");
			DeleteDbFiles.execute("./", name, true);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#getConnectString(java.lang.String)
	 */
	public String getConnectString(String name) {
		return String.format("jdbc:h2:mem:%s;LOCK_MODE=3", name);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#getConnection(java.lang.String)
	 */
	public Connection getConnection(String name) {
			try {
				return DriverManager.getConnection(getConnectString(name)); 
			} catch (Exception e) {
				throw new RuntimeException("Error connecting to " + name,e);
			}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.test.DatabaseContextInitializer#getDataSource(java.lang.String)
	 */
	public DataSource getDataSource(String name) {
		return new HiveBasicDataSource(getConnectString(name));
	}

	public void afterTest() {
		for(String name : names)
			createDatabase(name);
	}

	public void beforeTest() {
		for(String name: names)
			if(databaseExists(name))
				deleteDatabase(name);
	}

	public HiveDbDialect getDialect() {
		return HiveDbDialect.H2;
	}

	public String getHost() {
		return "";
	}

	public void setDatabaseNames(List<String> names) {
		this.names = names;
	}
}
