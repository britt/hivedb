package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

public class ContinentalSchema extends Schema {

	public ContinentalSchema() {
		super("Continent");
	}
	
	public ContinentalSchema(String dbURI) {
		super("Continent",dbURI);
	}

	public Collection<TableInfo> getTables() {
		return Arrays.asList(new TableInfo("CONTINENT", "CREATE TABLE CONTINENT (" + 
				"NAME VARCHAR(64) NOT NULL PRIMARY KEY," +
				"POPULATION INT);")
		);
	}
}
