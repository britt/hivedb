package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface ClassDaoService {
	@WebMethod
	ServiceResponse get(Serializable id);
	@WebMethod
	ServiceResponse getByReference(String property, Object referenceKey);
	@WebMethod
	ServiceResponse getByReferenceRange(String property, Object start, Object end);
	@WebMethod
	boolean exists(Serializable id);
	@WebMethod
	ServiceResponse save(Object obj);
	@WebMethod
	ServiceResponse saveAll(Collection<Object> instances);
	@WebMethod
	Object delete(Serializable id);
	@WebMethod
	Class<Object> getPersistedClass();
}
