package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

public class TemperatureSchema extends Schema {

	public TemperatureSchema(){
		super("Temperature");
	}
	
	public TemperatureSchema(String dbURI) {
		super("Temperature",dbURI);
	}

	@Override
	public Collection<TableInfo> getTables() {
		return Arrays.asList(new TableInfo("TEMPERATURE", "CREATE TABLE TEMPERATURE (" + 
    "TEMPERATURE_ID INT NOT NULL PRIMARY KEY," +
    "CONTINENT VARCHAR(50));"));	
	}
}
