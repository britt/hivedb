package org.hivedb.configuration.persistence;

import java.util.Collection;

public interface ConfigurationDataAccessObject<T> {
  Collection<T> loadAll();

  T create(T entity);

  T update(T entity);

  void delete(T entity);
}
