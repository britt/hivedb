package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

public interface ClassDaoService<T,ID extends Serializable> {
	T get(ID id);
	Collection<T> getAll(); // for debugging
	Collection<T> getByReference(String property, Object referenceKey);
	Collection<T> getByReferenceRange(String property, Object start, Object end);
	boolean exists(ID id);
	T save(T obj);
	Collection<T> saveAll(Collection<T> instances);
	ID delete(ID id);
	String getPersistedClass();
}
