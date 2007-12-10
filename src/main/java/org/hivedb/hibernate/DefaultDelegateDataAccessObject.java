package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;

import org.hibernate.Session;

public class DefaultDelegateDataAccessObject implements
		DelegateDataAccessObject<Object, Serializable> {

	public Serializable delete(Serializable id, Session session, Object deletedEntity) {
		return id;
	}

	public Collection<Object> findByProperty(String propertyName, Object value,
			Session session, Collection<Object> entities) {
		return entities;
	}

	public Object get(Serializable id, Session session, Object entity) {
		return entity;
	}

	public Object save(Object entity, Session session) {
		return entity;
	}

	public Collection<Object> saveAll(Collection<Object> collection, Session session) {
		return collection;
	}
}
