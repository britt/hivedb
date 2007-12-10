package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;

import org.hibernate.Session;

public interface DelegateDataAccessObject<T, ID extends Serializable> {

	Serializable delete(Serializable id, Session session, Object deletedEntity);

	T get(Serializable id, Session session, T entity);

	Collection<T> findByProperty(String propertyName, T value, Session session, Collection<T> entities);

	T save(T entity, Session session);
	
	Collection<T> saveAll(Collection<Object> collection, Session session);
}
