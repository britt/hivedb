package org.hivedb.util.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveRuntimeException;
import org.hivedb.persistence.DataSourceProvider;
import org.hivedb.util.Lists;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

public class H2Adapter implements DatabaseAdapter {
  private final static Log log = LogFactory.getLog(H2Adapter.class);
  private DataSourceProvider provider;
  private Collection<String> databasesCreated = Lists.newArrayList();

  public H2Adapter(DataSourceProvider provider) {
    this.provider = provider;
  }

  public void initializeDriver() {
    try {
      DriverLoader.loadByDialect(HiveDbDialect.H2);
    } catch (ClassNotFoundException e) {
      throw new HiveRuntimeException("Error initializing H2 driver.",e);
    }
  }

  public void createDatabase(String name) {
    try {
			getConnection(name).close();
      databasesCreated.add(name);
    } catch (SQLException e) {
			throw new HiveRuntimeException("Error creating database.",e);
		}
  }

  public void dropDatabase(String name) {
    try {
      getConnection(name).createStatement().execute("SHUTDOWN");
      databasesCreated.remove(name);
    } catch (SQLException e) {
			throw new HiveRuntimeException("Error shutting down database",e);
		}
  }

  public String getConnectString(String name) {
    return String.format("jdbc:h2:mem:%s;LOCK_MODE=3", name);
  }

  public Connection getConnection(String name) {
    try {
		  return provider.getDataSource(getConnectString(name)).getConnection();
		} catch (SQLException e) {
		  throw new HiveRuntimeException("Error creating connection",e);
		}
  }

  public DataSource getDataSource(String name) {
    return provider.getDataSource(getConnectString(name));
  }

  public boolean databaseExists(String name) {
    return databasesCreated.contains(name);
  }
}

