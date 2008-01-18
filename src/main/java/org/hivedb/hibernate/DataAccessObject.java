package org.hivedb.hibernate;

import java.util.Collection;

import org.hivedb.HiveKeyNotFoundException;

public interface DataAccessObject<T, ID> {
	/**
	 *  Retrieve the object with the given id
	 * @param id
	 * @return
	 * @throws HiveKeyNotFoundException Throws is the id does not exist
	 */
    public T get(ID id);
    
    /**
     *  Retrieve an object(s) by one of its indexed properties.
     * @param propertyName
     * @param value
     * @return
     */
    public Collection<T> findByProperty(String propertyName, Object value);
   
    public Collection<T> findByProperty(String propertyName, Object value, Integer firstResult, Integer maxResults);
    
    public Collection<T> findByPropertyRange(String propertyName, Object minValue, Object maxValue);
    
    public Collection<T> findByPropertyRange(String propertyName, Object minValue, Object maxValue, Integer firstResult, Integer maxResults);
    
    public Integer getCount(String propertyName, Object propertyValue);
    public Integer getCountByRange(String propertyName, Object minValue, Object maxValue);
    
    /**
     *  Create or Update this instance.
     * @param entity
     * @return An instance guaranteed to have an id. This reference need not be
     * the same object as the function argument, but should be equals and have the same hashCode().
     */
    public T save(T entity);
    
    /**
     *  Create or Update a collection of instances
     * @param collection
     * @return A collection of instances functionally equivalent to calling save(T obj) on each instance
     */
    public Collection<T> saveAll(Collection<T> collection);
    
    /**
     *  Delete the instance with the given id
     * @param id
     * @return The id deleted.
     * @throws Exception
     */
    public ID delete(ID id);
    
    /**
	 *  Returns true if the given id exists in the hive
	 * @param id
	 * @return
	 */
    public Boolean exists(ID id);
    
    /**
     *  Returns the class that this DAO persists
     * @return
     */
    public Class<T> getRespresentedClass();
}
