package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.meta.persistence.DataSourceProvider;

public class DbDirectoryFactory implements DirectoryFactory {
  private final static Log log = LogFactory.getLog(DbDirectoryFactory.class);
  private DataSourceProvider dataSourceProvider;

  public DbDirectoryFactory(DataSourceProvider provider) {
    this.dataSourceProvider = provider;
  }

  public Directory newInstance(HiveConfiguration config) {
    return new DbDirectory(config, dataSourceProvider.getDataSource(config.getUri()));
  }
}

