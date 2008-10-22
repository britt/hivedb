package org.hivedb.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Hive;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.Preconditions;
import org.hivedb.util.SynchronizedWrapper;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;

import java.util.Collection;

public class DelegatingHiveConfiguration implements HiveConfiguration {
  private final static Log log = LogFactory.getLog(DelegatingHiveConfiguration.class);
  private SynchronizedWrapper<HiveConfiguration> wrapper;

  public DelegatingHiveConfiguration(SynchronizedWrapper<HiveConfiguration> synchronizedInstance) {
    this.wrapper = synchronizedInstance;
  }

  public Collection<Node> getNodes() {
    return wrapper.get().getNodes();
  }

  public HiveSemaphore getSemaphore() {
    return wrapper.get().getSemaphore();
  }

  public PartitionDimension getPartitionDimension() {
    return wrapper.get().getPartitionDimension();
  }

  /**
   * {@inheritDoc}
   *
   * @param resourceName
   * @param hive
   */
  public boolean doesResourceExist(String resourceName, Hive hive) {
    return !Preconditions.isNameUnique(hive.getPartitionDimension().getResources(), resourceName);
  }

  public String getUri() {
    return wrapper.get().getUri();
  }

  public HiveDbDialect getDialect() {
    return DriverLoader.discernDialect(getUri());
  }
}

