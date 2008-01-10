package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebResult;
import javax.jws.WebService;

@WebService
public interface DataGenerationService {

	@WebMethod
	@WebResult
	public Collection<String> listClasses();

	@WebMethod
	@WebResult
	public Collection<Serializable> generate(String clazz, Integer partitionKeyCount, Integer instanceCount);
}