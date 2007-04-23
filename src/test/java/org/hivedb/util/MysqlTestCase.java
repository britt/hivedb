package org.hivedb.util;

import java.sql.DriverManager;
import java.util.Collection;

import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeMethod;

public class MysqlTestCase {
	
	protected Collection<String> dataNodes = null;
	@BeforeMethod
	public void setUp() {
		recycleDatabase();
		
		this.dataNodes = Transform.map(new Unary<Number, String>() {
			public String f(Number count) { return getDataNodeConnectString("data"+count.toString());  }},
			new NumberIterator(3));
		
		for (String dataNode : dataNodes)
			try {
				recycleDatabase(dataNode);
			}
			catch(Exception e) {}
	}
	private String getDataNodeConnectString(String name){
		return String.format("jdbc:mysql://localhost/%s?user=test&password=test",name);
	}
	
	protected String getDatabaseName() {
		return "test";
	}
	
	protected void recycleDatabase() {
		recycleDatabase(getDatabaseName());
	}
	
	protected void recycleDatabase(String databaseName) {
		
		java.sql.Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection( getDatabaseAgnosticConnectString() );
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		try {
			connection.prepareStatement("drop database " + databaseName).execute();
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to drop database " + databaseName,e);
		}
		try{
			connection.prepareStatement("create database " + databaseName).execute();
			connection.close();
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to drop database " + databaseName,e);
		}
	}
	
	protected String getDatabaseAgnosticConnectString() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}
	
	protected String getConnectString() {
		return "jdbc:mysql://localhost/"+getDatabaseName()+"?user=test&password=test";
	}
}
