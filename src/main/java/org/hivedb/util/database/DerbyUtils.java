package org.hivedb.util.database;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

public class DerbyUtils {
	public static final String derbyDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String derbyShutdownString = "jdbc:derby:;shutdown=true";
	
	public static Connection getConnection(String databaseName, Properties properties) throws InstantiationException, SQLException {
		return createConnection(connectString(databaseName), properties);
	}
	
	private static Connection createConnection(String connectString, Properties properties) throws SQLException, InstantiationException {
		try {
			getDriver(); //JNDI magic
		} catch (Exception e) {
			throw new InstantiationException(e.getMessage());
		}  
		return DriverManager.getConnection(connectString, properties);
	}
	
	//Alias for getConnection(String, Properties)
	public static Connection getConnection(String databaseName, String user, String password) throws InstantiationException, SQLException{
		Properties properties = new Properties();
		properties.setProperty("user", user);
		properties.setProperty("password", password);
		return getConnection(databaseName, properties);
	}
	
	public static Connection createDatabase(String databaseName, String user, String password) throws SQLException, InstantiationException {
		Properties properties = new Properties();
		properties.setProperty("user", user);
		properties.setProperty("password", password);
		return createConnection(getCreateString(databaseName), properties); 
	}
	
	public static void deleteDatabase(String basePath, String databaseName) throws IOException {
		shutdown(databaseName);
		File derbyDir = new File(basePath + File.separator + databaseName);
		if(derbyDir.exists())
			FileUtils.deleteDirectory(derbyDir);
	}
	
	//Kill all Derby DBs
	public static void shutdown() {
		try {
			DriverManager.getConnection(derbyShutdownString);
		} catch(SQLException e) {
		}
	}
	
	//Kill one particular Derby DB
	public static void shutdown(String databaseName) {
		try {
			DriverManager.getConnection(shutdownString(databaseName));
		} catch(SQLException e) {
		}
	}
	
	public static String getCreateString(String databaseName){
		StringBuilder cs = new StringBuilder("jdbc:derby:");
		cs.append(databaseName);
		cs.append(";create=true");
		return cs.toString();
	}
	
	public static String connectString(String databaseName){
		StringBuilder cs = new StringBuilder("jdbc:derby:");
		cs.append(databaseName);
		return cs.toString();
	}
	
	public static String shutdownString(String databaseName){
		StringBuilder cs = new StringBuilder("jdbc:derby:");
		cs.append(databaseName);
		cs.append(";shutdown=true");
		return cs.toString();
	}
	
	public static void executeScript(String sql, Connection conn) throws SQLException {
		String[] statements = sql.split(";");
		for(String statement : statements) {
			if(statement.length() > 1) //substitute for equals EOF
				conn.createStatement().execute(statement);
		}
	}
	
	public static void releaseConnection(Connection conn){
		try {
			if(!conn.isClosed())
				conn.close();
	    } catch (SQLException e) {
	    	throw new RuntimeException(e.getMessage());
	    }
	}
	
	private static Driver getDriver() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		return (Driver) Class.forName(derbyDriver).newInstance();
	}
}
