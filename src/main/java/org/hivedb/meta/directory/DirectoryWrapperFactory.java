package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Lockable;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;

import javax.sql.DataSource;
import java.util.Collection;

public class DirectoryWrapperFactory implements DirectoryFacadeProvider {
  private final static Log log = LogFactory.getLog(DirectoryWrapperFactory.class);
  private DirectoryProvider directoryProvider;
  private DataSourceProvider dataSourceProvider;

  public DirectoryWrapperFactory(DirectoryProvider directoryProvider, DataSourceProvider dataSourceProvider) {
    this.dataSourceProvider = dataSourceProvider;
    this.directoryProvider = directoryProvider;
  }

  public DirectoryFacade getDirectoryFacade(String hiveConfigurationUri, Assigner assigner, Lockable semaphore) {
    DataSource dataSource = dataSourceProvider.getDataSource(hiveConfigurationUri);
    PartitionDimension partitionDimension = new PartitionDimensionDao(dataSource).get();
    Collection<Node> nodes = new NodeDao(dataSource).loadAll();
    return new DirectoryWrapper(
      directoryProvider.getDirectory(hiveConfigurationUri),
      assigner,
      nodes,
      partitionDimension.getResources(),
      semaphore);
  }
}

