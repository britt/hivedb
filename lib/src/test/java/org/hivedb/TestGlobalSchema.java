package org.hivedb;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.GlobalSchema;
import org.hivedb.util.database.H2TestCase;
import org.testng.annotations.Test;

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
		GlobalSchema schema = new GlobalSchema(getConnectString(TEST_DB));
		schema.install();
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{TEST_DB});
	}
}
