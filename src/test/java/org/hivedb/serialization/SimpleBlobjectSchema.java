package org.hivedb.serialization;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Schema;
import org.hivedb.meta.persistence.TableInfo;

public class SimpleBlobjectSchema extends Schema {

	public SimpleBlobjectSchema(String dbURI) {
		super(dbURI);
	}

	@Override
	public Collection<TableInfo> getTables() {
		return Arrays.asList(
			new TableInfo(
				"BLOBJECT", 
				"CREATE TABLE BLOBJECT (" +
				"ID INT NOT NULL PRIMARY KEY, " +
				"MAPPED VARCHAR(255));"),
			new TableInfo(
				"BLOBJECT_BLOB", 
				"CREATE TABLE BLOBJECT_BLOB (" + 
				"ID INT NOT NULL PRIMARY KEY, " + 
				"DATA BLOB);")
		);
	}

}
