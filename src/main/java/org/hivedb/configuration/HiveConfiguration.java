package org.hivedb.configuration;

import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;

import java.util.Collection;

// TODO change the namespace on all of the entity persistence configurations
public interface HiveConfiguration {
  Collection<Node> getNodes();

  HiveSemaphore getSemaphore();

  PartitionDimension getPartitionDimension();
}
