package org.hivedb;

import org.hivedb.configuration.persistence.HiveConfigurationSchema;
import org.hivedb.util.database.test.H2TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

public class TestGlobalSchema extends H2TestCase {

	private static final String TEST_DB = "testDb";

	/**
	 * Execute all CREATE TABLE statements for global Hive schema.  Assume
	 * that no Exceptions means the create was successful.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInstallDerby() throws Exception {
		HiveConfigurationSchema schema = new HiveConfigurationSchema(getConnectString(TEST_DB));
		schema.install();
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{TEST_DB});
	}
}
