package org.hivedb.util.database.test;

import java.util.Collection;

import org.hivedb.Schema;

public interface SchemaInitializer {
	void initialize(Collection<Schema> schemaList);
}
