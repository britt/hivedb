package org.hivedb.util;

import java.sql.DriverManager;

import org.testng.annotations.BeforeMethod;

public class MysqlTestCase {
	
	@BeforeMethod
	public void setUp() {
		recycleDatabase();
	}
	
	protected String getDatabaseName() {
		return "test";
	}
	
	protected void recycleDatabase() {
		java.sql.Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection( getDatabaseAgnosticConnectString() );
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		try {
			connection.prepareStatement("drop database " + getDatabaseName()).execute();
		}
		catch (Exception e) {}
		try{
			connection.prepareStatement("create database " + getDatabaseName()).execute();
			connection.close();
		}
		catch (Exception e) {  }
	}
	
	protected String getDatabaseAgnosticConnectString() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}
	
	protected String getConnectString() {
		return "jdbc:mysql://localhost/"+getDatabaseName()+"?user=test&password=test";
	}
}
