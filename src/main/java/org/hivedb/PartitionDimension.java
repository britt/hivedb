package org.hivedb;

import java.util.Collection;

public interface PartitionDimension {
  int getColumnType();

  Integer getId();

  String getName();

  String getIndexUri();

  Collection<Resource> getResources();

  Resource getResource(String resourceName);

  boolean doesResourceExist(String resourceName);
}
