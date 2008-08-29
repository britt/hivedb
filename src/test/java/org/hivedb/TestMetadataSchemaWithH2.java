package org.hivedb;

import org.hivedb.configuration.HiveConfigurationSchema;
import org.hivedb.util.database.test.H2TestCase;
import org.junit.Test;

import java.util.Arrays;

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
