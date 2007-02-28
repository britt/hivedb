package org.hivedb;

import org.hivedb.meta.GlobalSchema;
import org.hivedb.util.DerbyTestCase;
import org.testng.annotations.Test;

public class TestMetadataSchemaWithDerby extends DerbyTestCase {
	/**
	 * Execute all CREATE TABLE statements for global Hive schema.  Assume
	 * that no Exceptions means the create was successful.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInstallDerby() throws Exception {
		// relies on getConnectString() from DerbyTest base class
		GlobalSchema schema = new GlobalSchema(getConnectString());
		schema.install();
	}	
}
