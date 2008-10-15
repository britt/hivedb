package org.hivedb.configuration;

import org.hivedb.meta.Node;

import java.util.Collection;

public interface HiveConfiguration {
  Collection<Node> getNodes();
}
