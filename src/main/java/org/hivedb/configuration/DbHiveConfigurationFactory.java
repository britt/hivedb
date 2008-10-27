package org.hivedb.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.util.Preconditions;
import org.hivedb.util.functional.Factory;

import javax.sql.DataSource;
import java.util.Collection;

public class DbHiveConfigurationFactory implements Factory<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(DbHiveConfigurationFactory.class);
  private DataSource dataSource;

  public DbHiveConfigurationFactory(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public HiveConfiguration newInstance() {
    HiveSemaphore semaphore = new HiveSemaphoreDao(dataSource).get();
    PartitionDimension partitionDimension = new PartitionDimensionDao(dataSource).get();
    Collection<Node> nodes = new NodeDao(dataSource).loadAll();

    Preconditions.isNotNull(semaphore, partitionDimension, nodes);
    return new HiveConfigurationImpl(nodes, partitionDimension, semaphore);
  }
}

