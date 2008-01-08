package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface ClassDaoService<T,ID extends Serializable> {
	@WebMethod
	ServiceResponse get(ID id);
	@WebMethod
	ServiceResponse getByReference(String property, Object referenceKey);
	@WebMethod
	ServiceResponse getByReferenceRange(String property, Object start, Object end);
	@WebMethod
	boolean exists(ID id);
	@WebMethod
	ServiceResponse save(T obj);
	@WebMethod
	ServiceResponse saveAll(Collection<T> instances);
	@WebMethod
	Object delete(ID id);
	@WebMethod
	String getPersistedClass();
}
