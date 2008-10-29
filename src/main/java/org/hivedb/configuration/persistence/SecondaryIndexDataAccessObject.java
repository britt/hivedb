package org.hivedb.configuration.persistence;

import org.hivedb.SecondaryIndex;

public interface SecondaryIndexDataAccessObject extends DataAccessObject<SecondaryIndex>{
  SecondaryIndex findByResourceId(Integer resourceId);
}
