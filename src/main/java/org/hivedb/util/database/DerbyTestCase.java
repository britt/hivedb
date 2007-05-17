package org.hivedb.util.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class DerbyTestCase {
	//Override these values to customize your test
	protected boolean cleanupDbAfterEachTest = false;
	protected String databaseName =  "derbyTestDb";
	protected String loadScript = null;
	protected String userName = "theuser";
	protected String password = "thepassword";
	protected boolean cleanupOnLoad = true;
	protected boolean cleanupOnExit = true;
	
	protected Collection<String> databaseNames;
	
	public DerbyTestCase() {
		databaseNames = new ArrayList<String>();
		databaseNames.add(getDatabaseName());
	}
	
	public DerbyTestCase(Collection<String> dbNames) {
		databaseNames = dbNames;
		if(!databaseNames.contains(databaseName))
			databaseNames.add(databaseName);
	}
	
	@SuppressWarnings("unused")
	@BeforeClass
	protected void initializeDerby(){
		if( cleanupOnLoad || cleanupDbAfterEachTest) {
			deleteDatabases();
		}
		for(String name : getDatabaseNames())
			createDatabase(name);
	}

	private void deleteDatabases() {
		for(String name : getDatabaseNames())
			deleteDerbyDb(name);
	}
	
	private void createDatabase(String name) {
		try {
			DerbyUtils.createDatabase(name, userName, password);
			if( loadScript != null && !"".equals(loadScript))
				loadFromSqlScript();
		} catch (IOException e) {
			throw new DerbyTestCaseException(
					"Error initializing the Derby database: IOException while reading the sql script.",e);
		} catch (Exception e) {
			throw new DerbyTestCaseException("Error initializing the Derby database: " + e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("unused")
	@AfterClass
	protected void cleanupDerby() {
		if( cleanupOnExit ){
			deleteDatabases();
		}
	}
	
	@BeforeMethod
	protected void beforeMethod() {
		if( cleanupDbAfterEachTest ){
			initializeDerby();
		}
	}
	@AfterMethod
	protected void afterMethod() {
		if( cleanupDbAfterEachTest ){
			deleteDatabases();
		}
	}

	
	protected String getConnectString()  {
		return DerbyUtils.connectString(getDatabaseName());
	}
	
	protected Connection getConnection() throws InstantiationException, SQLException {
		return DerbyUtils.getConnection(getDatabaseName(), userName, password);
	}
	
	protected DataSource getDataSource() {
		return new HiveBasicDataSource(getConnectString());
	}
	
	private void loadFromSqlScript() throws IOException, SQLException, InstantiationException {
		String sql = readFileAsString( relativePathToFullPath(loadScript));
		DerbyUtils.executeScript(sql, DerbyUtils.getConnection(getDatabaseName(), userName, password));
	}
	
	private void deleteDerbyDb(String name) {
		String path;
		try {
			path = new File(".").getCanonicalPath() + File.separator + name;
			File db = new File(path);
			if( db.exists())
				DerbyUtils.deleteDatabase(new File(".").getCanonicalPath(), name);
		} catch (IOException e) {
			throw new DerbyTestCaseException("Error deleting database", e);
		}
	}
	
	private static String relativePathToFullPath(String relativePath){
		String baseDir = null;
		try {
			baseDir = new File(".").getCanonicalPath();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baseDir + File.separator + relativePath;
	}
	
	private static String readFileAsString(String file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		StringBuilder sb = new StringBuilder();
		while((line = reader.readLine()) != null)
			sb.append(line + "\n");
		return sb.toString();
	}
	
	@SuppressWarnings("serial")
	class DerbyTestCaseException extends RuntimeException {
		public Throwable innerException;
		
		public DerbyTestCaseException(String message, Throwable t){
			//Don't ever set an empty message.  Apologies for the confusing syntax
			//but super() has to go first.
			super(message == null || "".equals(message) ? t.getMessage() : message);
			innerException = t;
		}
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	
	public Collection<String> getDatabaseNames() {
		return databaseNames;
	}
}
