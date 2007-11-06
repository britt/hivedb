package org.hivedb.hibernate;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

public class WeatherSchema extends Schema {

	public WeatherSchema(String dbURI) {
		super(dbURI);
	}

	@Override
	public Collection<TableInfo> getTables() {
		return Arrays.asList(new TableInfo("WEATHER_REPORT", "CREATE TABLE WEATHER_REPORT (" + 
    "REPORT_ID INT NOT NULL PRIMARY KEY," +
    "CONTINENT VARCHAR(50)," +
    "LATITUDE FLOAT," +
    "LONGITUDE FLOAT," +
    "TEMPERATURE INT," +
    "REPORT_TIME TIMESTAMP);"));
	}
}
