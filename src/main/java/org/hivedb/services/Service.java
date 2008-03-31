package org.hivedb.services;

import javax.jws.WebMethod;
import javax.jws.WebResult;

import org.hivedb.annotations.IndexParamPairs;

public interface Service<T,S extends ServiceResponse<T,C>,C extends ServiceContainer<T>, COL extends Iterable<T>, F> {
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1})
	public Integer getCountByProperty(String propertyName, String propertyValue);
	
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3})
	public Integer getCountByTwoProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2);

	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3,4,5})
	public Integer getCountByThreeProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2, String propertyName3, String propertyValue3);
}
