package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import javax.sql.DataSource;

import org.hivedb.management.JdbcKeyAuthority;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.MySQLMaxValueIncrementer;
import org.testng.annotations.BeforeMethod;

public class TestMysqlKeyAuthority {
	DataSource ds = null;

//	@Test(groups={"mysql"})
	public void testAssign() throws Exception {
		JdbcKeyAuthority<Integer> authority = new JdbcKeyAuthority<Integer>("key_authority_test",
				Integer.class);
		authority.setDataSource(getDataSource());
		authority.setIncrementer(getIncrementer());
		int firstKey = authority.nextAvailableKey().intValue();
		int secondKey = authority.nextAvailableKey().intValue();
		assertTrue(secondKey > firstKey);
	}
	
	@BeforeMethod()
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
		return "test_key_authority";
	}

	private DataFieldMaxValueIncrementer getIncrementer() {
		MySQLMaxValueIncrementer incrementer = new MySQLMaxValueIncrementer();
		incrementer.setDataSource(getDataSource());
		incrementer.setCacheSize(100);
		incrementer.setIncrementerName("key_authority_test");
		incrementer.setColumnName(JdbcKeyAuthority.COLUMN_NAME);
		return incrementer;
	}

	private DataSource getDataSource() {
		return ds;
	}
}
