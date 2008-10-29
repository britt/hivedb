package org.hivedb.util.database.test;

import org.hivedb.persistence.Schema;
import org.hivedb.persistence.TableInfo;

import java.util.Arrays;
import java.util.Collection;

public class ContinentalSchema extends Schema {
	
	private static ContinentalSchema INSTANCE = new ContinentalSchema();
	
	private ContinentalSchema() {
		super("Continent");
	}

	public Collection<TableInfo> getTables(String uri) {
		return Arrays.asList(new TableInfo("CONTINENT", "CREATE TABLE CONTINENT (" +
				"NAME VARCHAR(64) NOT NULL PRIMARY KEY," +
				"POPULATION INT);")
		);
	}
	
	public static ContinentalSchema getInstance() {
		return INSTANCE;
	}
}
