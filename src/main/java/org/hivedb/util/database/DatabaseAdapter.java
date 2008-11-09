package org.hivedb.util.database;

import javax.sql.DataSource;
import java.sql.Connection;

public interface DatabaseAdapter {
  void initializeDriver();
  void createDatabase(String name); 
  void dropDatabase(String name);
  String getConnectString(String name);
  Connection getConnection(String name);
  DataSource getDataSource(String name);
  boolean databaseExists(String name);
}
