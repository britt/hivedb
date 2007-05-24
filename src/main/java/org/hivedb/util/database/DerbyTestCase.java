package org.hivedb.util.database;

import java.io.File;
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
	protected boolean cleanupDbAfterEachTest = true;
	protected String databaseName =  "derbyTestDb";
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
			databaseNames.add(getDatabaseName());
	}
	
	@SuppressWarnings("unused")
	@BeforeClass
	protected void beforeClass(){
		if( cleanupOnLoad || cleanupDbAfterEachTest) {
			for(String name : getDatabaseNames()){
				deleteDerbyDb(name);
				createDatabase(name);
			}
		}
	}
	
	@BeforeMethod
	protected void beforeMethod() {
		if( cleanupDbAfterEachTest ) {
			for(String name : getDatabaseNames()){
				createDatabase(name);
			}
		}
	}

	@SuppressWarnings("unused")
	@AfterClass
	protected void afterClass() {
		if( cleanupOnExit ){
			for(String name : getDatabaseNames()){
				deleteDerbyDb(name);
			}
		}
		DerbyUtils.shutdown();
	}
	
	@AfterMethod
	protected void afterMethod() {
		if( cleanupDbAfterEachTest ){
			for(String name : getDatabaseNames()){
				deleteDerbyDb(name);
				DerbyUtils.shutdown(name);
			}
		}
	}
	
	private void createDatabase(String name) {
		try {
			DerbyUtils.createDatabase(name, userName, password);
		} catch (Exception e) {
			throw new DerbyTestCaseException("Error initializing the Derby database: " + e.getMessage(), e);
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
