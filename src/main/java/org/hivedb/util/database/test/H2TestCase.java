package org.hivedb.util.database.test;

import org.hivedb.persistence.Schema;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.NodeImpl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

/**
 *  This class adds and overrides behaviour of DatabaseTestCase
 *  specific to the needs of H2
 * @author andylikuski
 *
 */
public class H2TestCase extends DatabaseTestCase {
	public static final String TEST_DB = "testDb";
	private static Map<String, Boolean> databaseCreated = new Hashtable<String, Boolean>();
	
	static {
		try {
			Class.forName("org.h2.Driver").newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error initalizing the h2 server.", e);
		}
	}
	
	protected Collection<Schema> getSchemas() {
		return Collections.emptyList();
	}
	
	@Override
	protected void createDatabase(String name) {
		try {
			databaseCreated.put(name, true);
			getConnection(name).close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected boolean databaseExists(String name) {
		return databaseCreated.containsKey(name);
	}

	@Override
	protected void deleteDatabase(final String name) {
		try {
      getConnection(name).createStatement().execute("SHUTDOWN");
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
		return CachingDataSourceProvider.getInstance().getDataSource(getConnectString(name));
	}

	@Override
	protected Collection<NodeImpl> getDataNodes() {
		return Collections.emptyList();
	}
}
