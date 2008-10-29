package org.hivedb.meta.persistence;

import org.hivedb.meta.SecondaryIndex;

public interface SecondaryIndexDataAccessObject extends DataAccessObject<SecondaryIndex>{
  SecondaryIndex findByResourceId(Integer resourceId);
}
