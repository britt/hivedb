package org.hivedb.util.database.test;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

import javax.sql.DataSource;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

@SuppressWarnings("unchecked")
public abstract class DatabaseTestCase {
	protected boolean cleanupAfterEachTest = true;
	protected boolean cleanupOnExit = true;	
	protected boolean createDatabaseIfNoCleanup = true; // overridden by cleanup* flags
	private Collection<String> databaseNames = Collections.EMPTY_LIST;
	
	protected abstract void createDatabase(String name);
	protected abstract void deleteDatabase(String name);
	protected abstract boolean databaseExists(String name);
	protected abstract String getConnectString(String name);
	protected abstract Connection getConnection(String name);
	protected abstract DataSource getDataSource(String name);
	
	@BeforeClass
	protected void beforeClass(){
		
	}

	@BeforeMethod
	protected void beforeMethod() {
		if( cleanupAfterEachTest ) {
			for(String name : getDatabaseNames()){
				if(databaseExists(name)) {
					deleteDatabase(name);
					createDatabase(name);
				} else
					createDatabase(name);
			}
		}
		else
			if (createDatabaseIfNoCleanup)
				for(String name : getDatabaseNames())
					createDatabase(name);
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

	/**
	 * The name of all databases that need to be created and deleted by this test
	 * @return
	 */
	public Collection<String> getDatabaseNames() {
		return this.databaseNames;
	}
	
	public void setDatabaseNames(Collection<String> names) {
		this.databaseNames = names;
	}
}
