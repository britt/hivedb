package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import javax.sql.DataSource;

import org.hivedb.management.KeyAuthority;
import org.hivedb.management.MySqlKeyAuthority;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class TestMysqlKeyAuthority {
	DataSource ds = null;

	//@Test()
	@SuppressWarnings("unchecked")
	public void testAssign() throws Exception {
		KeyAuthority<Integer> authority = new MySqlKeyAuthority<Integer>(
				getDataSource(), this.getClass(), Integer.class);
		int firstKey = authority.nextAvailableKey().intValue();
		int secondKey = authority.nextAvailableKey().intValue();
		assertTrue(secondKey > firstKey);
	}

	//@BeforeMethod
	public void configure() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		ds = new HiveBasicDataSource(getConnectStringWithoutDatabase());
		try {
			ds.getConnection().createStatement().execute(
					"DROP DATABASE " + getDatabaseName());
		} catch (Exception ex) { // ok
		}
		ds.getConnection().createStatement().execute(
				"CREATE DATABASE " + getDatabaseName());
		ds = new HiveBasicDataSource(getConnectString());
	}

	private String getConnectStringWithoutDatabase() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}

	private String getConnectString() {
		return "jdbc:mysql://localhost/" + getDatabaseName()
				+ "?user=test&password=test";
	}

	private String getDatabaseName() {
		return "test";
	}

	private DataSource getDataSource() {
		return ds;
	}
}
