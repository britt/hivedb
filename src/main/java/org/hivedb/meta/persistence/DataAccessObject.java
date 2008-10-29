package org.hivedb.meta.persistence;

import java.util.Collection;

public interface DataAccessObject<T> {
  Collection<T> loadAll();
  T create(T entity);
  T update(T entity);
  void delete(T entity);
}
