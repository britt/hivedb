package org.hivedb.configuration;

import org.hivedb.HiveSemaphore;
import org.hivedb.Node;
import org.hivedb.PartitionDimension;
import org.hivedb.util.database.HiveDbDialect;

import java.util.Collection;

public interface HiveConfiguration {
  Collection<Node> getNodes();

  HiveSemaphore getSemaphore();

  PartitionDimension getPartitionDimension();

  String getUri();

  HiveDbDialect getDialect();
}
