package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

public interface ClassDaoService {
	ServiceResponse get(Serializable id);
	ServiceResponse getByReference(String property, Object referenceKey);
	ServiceResponse getByReferenceRange(String property, Object start, Object end);
	boolean exists(Serializable id);
	ServiceResponse save(Object obj);
	ServiceResponse saveAll(Collection<Object> instances);	
	Object delete(Serializable id);
	Class<Object> getPersistedClass();
}
