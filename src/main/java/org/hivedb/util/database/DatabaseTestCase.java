package org.hivedb.util.database;

import java.sql.Connection;
import java.util.Collection;

import javax.sql.DataSource;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class DatabaseTestCase {
	boolean cleanupAfterEachTest = true;
	boolean cleanupOnExit = true;

	protected String userName = "test";
	protected String password = "test";
	
	private Collection<String> databaseNames;
	
	protected abstract void createDatabase(String name);
	protected abstract void deleteDatabase(String name);
	protected abstract boolean databaseExists(String name);
	protected abstract String getConnectString(String name);
	protected abstract Connection getConnection(String name);
	protected abstract DataSource getDataSource(String name);
	
	@BeforeClass
	protected void beforeClass(){
		for(String name : getDatabaseNames()){
			if(databaseExists(name)) {
				deleteDatabase(name);
				createDatabase(name);
			}
		}
	}

	@BeforeMethod
	protected void beforeMethod() {
		if( cleanupAfterEachTest ) {
			for(String name : getDatabaseNames()){
				if(!databaseExists(name))
					createDatabase(name);
			}
		}
	}
	
	@AfterClass
	protected void afterClass() {
		if( cleanupOnExit ){
			for(String name : getDatabaseNames()){
				if(databaseExists(name))
					deleteDatabase(name);
			}
		}
	}
	
	@AfterMethod
	protected void afterMethod() {
		if( cleanupAfterEachTest ){
			for(String name : getDatabaseNames()){
				if(databaseExists(name))
					deleteDatabase(name);
			}
		}
	}

	public Collection<String> getDatabaseNames() {
		return this.databaseNames;
	}
	
	public void setDatabaseNames(Collection<String> names) {
		this.databaseNames = names;
	}
}
