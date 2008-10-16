package org.hivedb.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;

import java.util.Collection;

public class HiveConfigurationImpl implements HiveConfiguration {
  private final static Log log = LogFactory.getLog(HiveConfigurationImpl.class);
  private Collection<Node> nodes;
  private HiveSemaphore semaphore;
  private PartitionDimension partitionDimension;

  public HiveConfigurationImpl(Collection<Node> nodes, PartitionDimension partitionDimension, HiveSemaphore semaphore) {
    this.nodes = nodes;
    this.partitionDimension = partitionDimension;
    this.semaphore = semaphore;
  }

  public Collection<Node> getNodes() {
    return nodes;
  }

  public PartitionDimension getPartitionDimension() {
    return partitionDimension;
  }

  public HiveSemaphore getSemaphore() {
    return semaphore;
  }
}

