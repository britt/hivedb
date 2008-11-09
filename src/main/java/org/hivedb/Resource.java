package org.hivedb;

import org.hivedb.util.IdAndNameIdentifiable;

import java.util.Collection;

public interface Resource extends IdAndNameIdentifiable<Integer> {  
  boolean isPartitioningResource();
  void setPartitioningResource(boolean b);
  
  Collection<SecondaryIndex> getSecondaryIndexes();

  SecondaryIndex getSecondaryIndex(String secondaryIndexName);

  ResourceIndex getIdIndex();

  int getColumnType();

  Integer getId();
  void setId(Integer id);

}
