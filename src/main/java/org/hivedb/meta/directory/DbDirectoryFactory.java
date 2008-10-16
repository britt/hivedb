package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.util.functional.Factory;

import javax.sql.DataSource;

public class DbDirectoryFactory implements Factory<Directory> {
  private final static Log log = LogFactory.getLog(DbDirectoryFactory.class);
  private DataSource dataSource;

  public DbDirectoryFactory(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public Directory newInstance() {
    return new DbDirectory(new PartitionDimensionDao(dataSource).get(), dataSource);
  }
}

