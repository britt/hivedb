package org.hivedb.services;

import java.util.Collection;

import org.hivedb.annotations.GeneratedClass;
import org.hivedb.util.database.test.WeatherReport;

@GeneratedClass("WeatherServiceResponseGenerated")
public interface WeatherServiceResponse  extends ServiceResponse<WeatherReport, WeatherServiceContainer> {
	public Collection<WeatherServiceContainer> getContainers();
}
