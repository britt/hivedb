package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.functional.Factory;

import javax.sql.DataSource;

public class DbDirectoryFactory implements Factory<Directory> {
  private final static Log log = LogFactory.getLog(DbDirectoryFactory.class);
  private DataSource dataSource;
  private HiveConfiguration hiveConfiguration;

  public DbDirectoryFactory(DataSource dataSource, HiveConfiguration hiveConfiguration) {
    this.dataSource = dataSource;
    this.hiveConfiguration = hiveConfiguration;
  }

  public Directory newInstance() {
    return new DbDirectory(hiveConfiguration, dataSource);
  }
}

