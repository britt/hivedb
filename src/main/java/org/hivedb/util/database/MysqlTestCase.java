package org.hivedb.util.database;

import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeMethod;

public class MysqlTestCase {
	protected boolean recycleDatabase = true;

	// Data nodes are inserted in the hive's node_metadata table. The databases
	// are never actually created,
	// since the hive never interacts directly with the data nodes.
	protected String[] dataNodes = new String[] { "data1", "data2", "data3" };

	public Collection<String> getDataUris() {
		return Transform.map(new Unary<String, String>() {
			public String f(String dataNodeName) {
				return getDataNodeConnectString(dataNodeName);
			}
		}, Arrays.asList(dataNodes));
	}

	@BeforeMethod
	public void setUp() {
		recycleDatabase();
	}

	protected String getDataNodeConnectString(String name) {
		return String.format(
				"jdbc:mysql://localhost/%s?user=test&password=test", name);
	}

	protected String getDatabaseName() {
		return "test";
	}

	protected void recycleDatabase() {
		recycleDatabase(getDatabaseName());
	}

	protected void recycleDatabase(String databaseName) {
		if (recycleDatabase) {
			java.sql.Connection connection = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				connection = DriverManager
						.getConnection(getDatabaseAgnosticConnectString());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			try {
				connection.prepareStatement("drop database " + databaseName)
						.execute();
			} catch (Exception e) {
				throw new RuntimeException("Unable to drop database "
						+ databaseName, e);
			}
			try {
				connection.prepareStatement("create database " + databaseName)
						.execute();
				connection.close();
			} catch (Exception e) {
				throw new RuntimeException("Unable to drop database "
						+ databaseName, e);
			}
		}
	}

	protected String getDatabaseAgnosticConnectString() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}

	protected String getConnectString() {
		return "jdbc:mysql://localhost/" + getDatabaseName()
				+ "?user=test&password=test";
	}
}
