package org.hivedb.util.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.persistence.Schema;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Maps;

import java.util.Collection;
import java.util.Map;

public class DatabaseInitializer {
  private final static Log log = LogFactory.getLog(DatabaseInitializer.class);
  private DatabaseAdapter adapter;
  private Map<String, Collection<Schema>> schemaMap = Maps.newHashMap();

  public DatabaseInitializer(DatabaseAdapter adapter) {
    this.adapter = adapter;
  }

  public void initializeDatabases() {
    for (Map.Entry<String, Collection<Schema>> entry : schemaMap.entrySet()) {
      String name = entry.getKey();
      Collection<Schema> schemas = entry.getValue();
      if (!adapter.databaseExists(name)) {
        adapter.createDatabase(name);
      }
      for (Schema schema : schemas) {
        Schemas.install(schema, adapter.getConnectString(name));
      }
    }
  }

  public void clearData() {
    for (Map.Entry<String, Collection<Schema>> entry : schemaMap.entrySet()) {
      String name = entry.getKey();
      Collection<Schema> schemas = entry.getValue();
      for (Schema schema : schemas) {
        Schemas.emptyTables(schema, adapter.getConnectString(name));
      }
    }
  }

  public void destroyDatabases() {
    for (String dbName : schemaMap.keySet()) {
      if (adapter.databaseExists(dbName)) {
        adapter.dropDatabase(dbName);
      }
    }
  }

  public void addDatabase(String name, Schema... schemas) {
    schemaMap.put(name, Lists.newList(schemas));
  }

  public Collection<String> getDatabases() {
    return schemaMap.keySet();
  }
}

