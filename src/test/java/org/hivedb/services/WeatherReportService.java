package org.hivedb.services;

import java.util.Collection;

import javax.jws.WebMethod;
import javax.jws.WebResult;
import javax.jws.WebService;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.annotations.IndexParamPagingPair;
import org.hivedb.annotations.IndexParamPairs;
import org.hivedb.util.database.test.WeatherReport;

@GeneratedClass("WeatherReportServiceGenerated")
@WebService
public interface WeatherReportService extends Service<WeatherReport, WeatherServiceResponse, WeatherServiceContainer, Collection<WeatherReport>, Integer>  {
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1})
	public WeatherServiceResponse findByProperty(String propertyName, String propertyValue);

	@WebMethod
	@WebResult
	@IndexParamPairs({0,1})
	public Integer getCountByProperty(String propertyName, String propertyValue);

	@WebMethod
	@WebResult
	@IndexParamPairs({0,1})
	@IndexParamPagingPair(startIndexIndex=2, maxResultsIndex=3)
	public WeatherServiceResponse findByPropertyPaged(String propertyName, String propertyValue, Integer firstResult, Integer maxResults);
	
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3})
	public WeatherServiceResponse findByTwoProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2);

	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3})
	public Integer getCountByTwoProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2);

	
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3})
	@IndexParamPagingPair(startIndexIndex=4, maxResultsIndex=5)
	public WeatherServiceResponse findByTwoPropertiesPaged(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2, Integer firstResult, Integer maxResults);
	
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3,4,5})
	public WeatherServiceResponse findByThreeProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2, String propertyName3, String propertyValue3);

	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3,4,5})
	public Integer getCountByThreeProperties(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2, String propertyName3, String propertyValue3);

	
	@WebMethod
	@WebResult
	@IndexParamPairs({0,1,2,3,4,5})
	@IndexParamPagingPair(startIndexIndex=6, maxResultsIndex=7)
	public WeatherServiceResponse findByThreePropertiesPaged(String propertyName1, String propertyValue1, String propertyName2, String propertyValue2, String propertyName3, String propertyValue3, Integer firstResult, Integer maxResults);
	
	@WebMethod
	@WebResult
	public WeatherServiceResponse get(Long id);
	@WebMethod
	@WebResult
	public boolean exists(Long id);
	@WebMethod
	@WebResult
	public WeatherServiceResponse save(WeatherReport obj);
	@WebMethod
	@WebResult
	public WeatherServiceResponse saveAll(WeatherReport[] instances);
	@WebMethod
	@WebResult
	public Object delete(Long id);
	@WebMethod
	@WebResult
	public String getPersistedClass();
}
