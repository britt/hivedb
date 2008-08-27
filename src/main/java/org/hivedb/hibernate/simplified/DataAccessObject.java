package org.hivedb.hibernate.simplified;

import java.util.Collection;
import java.util.Map;

public interface DataAccessObject<T, ID> {
  /**
   * Retrieve the object with the given id
   *
   * @param id
   * @return The object with the given id
   * @throws HiveKeyNotFoundException Throws is the id does not exist
   */
  public T get(ID id);

  /**
   * Returns true if the given id exists in the hive (a corresponding entity
   * does not necessarily exist on the data node).
   *
   * @param id
   * @return
   */
  public Boolean exists(ID id);

  /**
   * Retrieve entities by property.
   *
   * @param properties A map of property names to values
   * @return A collection of matching objects
   */
  public Collection<T> find(Map<String, Object> properties);

  /**
   * Retrieve entities by property with paging.
   *
   * @param properties A map of property names to values
   * @param offSet
   * @param maxResultSetSize
   * @return A collection of matching objects
   */
  public Collection<T> find(Map<String, Object> properties, Integer offSet, Integer maxResultSetSize);

  public Collection<T> findInRange(String propertyName, Object minValue, Object maxValue);

  public Collection<T> findInRange(String propertyName, Object minValue, Object maxValue, Integer offSet, Integer maxResultSetSize);

  public Integer getCount(Map<String, Object> properties);

  public Integer getCountInRange(String propertyName, Object minValue, Object maxValue);

  /**
   * Create or Update this instance.
   *
   * @param entity
   * @return An instance guaranteed to have an id. This reference need not be
   *         the same object as the function argument, but should be equals and have the same hashCode().
   */
  public T save(T entity);

  /**
   * Create or Update a collection of instances
   *
   * @param collection
   * @return A collection of instances functionally equivalent to calling save(T obj) on each instance
   */
  public Collection<T> saveAll(Collection<T> collection);

  /**
   * Delete the instance with the given id
   *
   * @param id
   * @return The id deleted.
   * @throws Exception
   */
  public ID delete(ID id);

  /**
   * Returns the class that this DAO persists
   *
   * @return
   */
  public Class<T> getRespresentedClass();
}
