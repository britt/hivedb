package org.hivedb.util;

import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlTestCase {
	
	protected String getDatabaseName() {
		return "test";
	}
	protected void recycleDatabase(String databaseName, String databaseAgnosticConnectionString) throws SQLException {
		java.sql.Connection connection  = DriverManager.getConnection( databaseAgnosticConnectionString );
		try {
			connection.prepareStatement("drop database " + databaseName).execute();
		}
		catch (Exception e) {}
		connection.prepareStatement("create database " + databaseName).execute();
		connection.close();
	}
	
	protected String getDatabaseAgnosticConnectString() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}
	
	protected String getConnectString(String database) {
		return "jdbc:mysql://localhost/"+database+"?user=test&password=test";
	}
}
