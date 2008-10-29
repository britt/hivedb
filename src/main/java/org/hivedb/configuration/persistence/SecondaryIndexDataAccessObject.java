package org.hivedb.configuration.persistence;

import org.hivedb.SecondaryIndex;

public interface SecondaryIndexDataAccessObject extends ConfigurationDataAccessObject<SecondaryIndex> {
  SecondaryIndex findByResourceId(Integer resourceId);
}
