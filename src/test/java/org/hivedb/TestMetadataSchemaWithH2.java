package org.hivedb;

import java.util.Arrays;

import org.hivedb.meta.HiveConfigurationSchema;
import org.hivedb.util.database.H2TestCase;
import org.testng.annotations.Test;

public class TestMetadataSchemaWithH2 extends H2TestCase {
	/**
	 * Execute all CREATE TABLE statements for global Hive schema.  Assume
	 * that no Exceptions means the create was successful.
	 * 
	 * @throws Exception
	 */
	public TestMetadataSchemaWithH2() {
		this.setDatabaseNames(Arrays.asList(new String[]{"testDb"}));
	}
	
	@Test
	public void testInstallDerby() throws Exception {
		// relies on getConnectString() from DerbyTest base class
		HiveConfigurationSchema schema = new HiveConfigurationSchema(getConnectString("testDb"));
		schema.install();
	}	
}
