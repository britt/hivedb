package org.hivedb.util.database.test;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

import java.util.Arrays;
import java.util.Collection;

public class TemperatureSchema extends Schema {
	
	private static TemperatureSchema INSTANCE = new TemperatureSchema();
	
	private TemperatureSchema(){
		super("Temperature");
	}
	
	@Override
	public Collection<TableInfo> getTables(String uri) {
		return Arrays.asList(new TableInfo("TEMPERATURE", "CREATE TABLE TEMPERATURE (" + 
    "TEMPERATURE_ID INT NOT NULL PRIMARY KEY," +
    "CONTINENT VARCHAR(50));"));	
	}

	public static TemperatureSchema getInstance() {
		return INSTANCE;
	}
}
