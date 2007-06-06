package org.hivedb;

import java.util.Arrays;

import org.hivedb.meta.GlobalSchema;
import org.hivedb.util.database.DerbyTestCase;
import org.testng.annotations.Test;

public class TestMetadataSchemaWithDerby extends DerbyTestCase {
	/**
	 * Execute all CREATE TABLE statements for global Hive schema.  Assume
	 * that no Exceptions means the create was successful.
	 * 
	 * @throws Exception
	 */
	public TestMetadataSchemaWithDerby() {
		this.setDatabaseNames(Arrays.asList(new String[]{"testDb"}));
	}
	
	@Test
	public void testInstallDerby() throws Exception {
		// relies on getConnectString() from DerbyTest base class
		GlobalSchema schema = new GlobalSchema(getConnectString("testDb"));
		schema.install();
	}	
}
