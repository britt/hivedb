package org.hivedb.annotations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveSemaphore;
import org.hivedb.Node;
import org.hivedb.PartitionDimension;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.configuration.HiveConfigurationImpl;
import org.hivedb.configuration.persistence.HiveSemaphoreDao;
import org.hivedb.configuration.persistence.NodeDao;
import org.hivedb.configuration.persistence.PartitionDimensionDao;
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

