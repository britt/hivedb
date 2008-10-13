package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.meta.persistence.PartitionDimensionDao;

import javax.sql.DataSource;

public class DbDirectoryFactory implements DirectoryProvider {
  private final static Log log = LogFactory.getLog(DbDirectoryFactory.class);
  private DataSourceProvider provider;

  public DbDirectoryFactory(DataSourceProvider provider) {
    this.provider = provider;
  }

  public DbDirectory getDirectory(String uri) {
    DataSource dataSource = provider.getDataSource(uri);
    return new DbDirectory(new PartitionDimensionDao(dataSource).get(), dataSource);
  }
}

