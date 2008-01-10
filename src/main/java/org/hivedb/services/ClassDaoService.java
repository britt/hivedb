package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebService;

public interface ClassDaoService<T,ID extends Serializable> {
	ServiceResponse get(ID id);
	ServiceResponse getByReference(String property, Object referenceKey);
	ServiceResponse getByReferenceRange(String property, Object start, Object end);
	boolean exists(ID id);
	ServiceResponse save(T obj);
	ServiceResponse saveAll(Collection<T> instances);
	Object delete(ID id);
	String getPersistedClass();
}
