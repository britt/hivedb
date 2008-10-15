package org.hivedb.meta;

import java.util.Collection;

public interface Resource extends IdAndNameIdentifiable<Integer> {
  PartitionDimension getPartitionDimension();

  boolean isPartitioningResource();

  Collection<SecondaryIndex> getSecondaryIndexes();

  SecondaryIndex getSecondaryIndex(String secondaryIndexName);

  ResourceIndex getIdIndex();

  int getColumnType();
}
