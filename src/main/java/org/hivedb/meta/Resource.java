package org.hivedb.meta;

import java.util.Collection;

public interface Resource extends IdAndNameIdentifiable<Integer> {
  PartitionDimension getPartitionDimension();
  void setPartitionDimension(PartitionDimension dimension);
  
  boolean isPartitioningResource();
  void setPartitioningResource(boolean b);
  
  Collection<SecondaryIndex> getSecondaryIndexes();

  SecondaryIndex getSecondaryIndex(String secondaryIndexName);

  ResourceIndex getIdIndex();

  int getColumnType();

  Integer getId();
  void setId(Integer id);

}
