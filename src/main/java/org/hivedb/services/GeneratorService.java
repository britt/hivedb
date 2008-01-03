package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

@WebService
public interface GeneratorService {

	@WebMethod
	public Collection<String> listClasses();

	@WebMethod
	public Collection<Serializable> generate(@WebParam(name="className") String clazz, @WebParam(name="partitionKeyCount") Integer partitionKeyCount,@WebParam(name="instanceCount") Integer instanceCount);

}