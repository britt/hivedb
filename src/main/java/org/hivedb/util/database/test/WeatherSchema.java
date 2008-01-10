package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

public class WeatherSchema extends Schema {

	public WeatherSchema(){
		super("WeatherReport");
	}
	
	public WeatherSchema(String dbURI) {
		super("WeatherReport",dbURI);
	}

	@Override
	public Collection<TableInfo> getTables() {
		return Arrays.asList(new TableInfo("WEATHER_REPORT", "CREATE TABLE WEATHER_REPORT (" + 
    "REPORT_ID VARCHAR(50) NOT NULL PRIMARY KEY," +
    "CONTINENT VARCHAR(50)," +
    "REGION_CODE INT," +
    "LATITUDE DOUBLE," +
    "LONGITUDE DOUBLE," +
    "TEMPERATURE INT," +
    "REPORT_TIME TIMESTAMP);"),
    	// Demonstrates a primitive indexed collection
    	new TableInfo("SOURCE", "CREATE TABLE WEATHER_REPORT_SOURCE (" + 
    	    "REPORT_ID INT NOT NULL ," +
    	    "SOURCE INT);"),
    	// Demonstrates a complex indexed collection (one-to-many)
	    new TableInfo("WEATHER_EVENT", "CREATE TABLE WEATHER_EVENT (" + 
	    	    "EVENT_ID INT NOT NULL ," +
	    	    "REPORT_ID INT NOT NULL ," +
	    	    "CONTINENT VARCHAR(50) NOT NULL ," +
	    	    "NAME VARCHAR(50));"),
	    // Demonstrates an unindexed data table of an non-entity class (WeatherEvent)
	    new TableInfo("EVENT_STATISTIC", "CREATE TABLE EVENT_STATISTIC (" + 
	    	    "EVENT_ID INT NOT NULL," +
	    	    "STAT INT);"));
	}
}
