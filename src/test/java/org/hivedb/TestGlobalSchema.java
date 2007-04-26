package org.hivedb;

import org.hivedb.meta.GlobalSchema;
import org.hivedb.util.database.DerbyTestCase;

public class TestGlobalSchema extends DerbyTestCase {

	/**
	 * Execute all CREATE TABLE statements for global Hive schema.  Assume
	 * that no Exceptions means the create was successful.
	 * 
	 * @throws Exception
	 */
	public void testInstallDerby() throws Exception {
		// relies on getConnectString() from DerbyTest base class
		GlobalSchema schema = new GlobalSchema(getConnectString());
		schema.install();
	}
	
	/**
	 * Supply a valid connect string & rename to test MySql.  Please do not
	 * check in as a running test.
	 */
	public void disabledTestInstallMySql() throws Exception {
	//public void testInstallMySql() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		String connectString = "jdbc:mysql://localhost/test?user=test&password=test";
		GlobalSchema schema = new GlobalSchema(connectString);
		schema.install();
	}
}
