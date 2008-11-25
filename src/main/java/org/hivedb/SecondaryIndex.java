package org.hivedb;

import org.hivedb.persistence.ColumnInfo;
import org.hivedb.util.IdAndNameIdentifiable;

public interface SecondaryIndex extends IdAndNameIdentifiable<Integer> {
  Integer getId();
  void setId(Integer id);

  String getName();

  ColumnInfo getColumnInfo();

  Resource getResource();

  void setResource(Resource resource);
}
