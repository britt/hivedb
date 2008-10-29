package org.hivedb.util.database.test;

import org.hivedb.persistence.Schema;
import org.hivedb.Node;
import org.hivedb.util.database.Schemas;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

@SuppressWarnings("unchecked")
public abstract class DatabaseTestCase {
	protected boolean cleanupAfterEachTest = true;
	protected boolean cleanupOnExit = true;	
	protected boolean createDatabaseIfNoCleanup = true; // overridden by cleanup* flags
	protected boolean deleteDatabasesAfterEachTest = false;
	private Collection<String> databaseNames = Collections.EMPTY_LIST;
	
	protected abstract void createDatabase(String name);
	protected abstract void deleteDatabase(String name);
	protected abstract boolean databaseExists(String name);
	protected abstract String getConnectString(String name);
	protected abstract Connection getConnection(String name);
	protected abstract DataSource getDataSource(String name);
	protected abstract Collection<Schema> getSchemas();
	protected abstract Collection<Node> getDataNodes();
		
	/**
	 * At the end of this method all databases and tables must exist and be empty
	 */
	@Before
	public void beforeMethod() {
		if( cleanupAfterEachTest ) {
			for(String name : getDatabaseNames()){
				if(databaseExists(name)) {
					clearTablesOfDatabase(name);
				} else
					createDatabase(name);
			}
		}
		else
			if (createDatabaseIfNoCleanup)
				for(String name : getDatabaseNames())
					createDatabase(name);
	}
	
	/**
	 * At the end of this method all databases and tables must exist and be empty
	 */
	@After
	public void afterMethod() {
		if( cleanupAfterEachTest ){
			for(String name : getDatabaseNames()){
				if(databaseExists(name)) {
					clearTablesOfDatabase(name);
					if (deleteDatabasesAfterEachTest) {
						deleteDatabase(name);
					}
				}
			}
		}
	}
	
	/**
	 * At the end of this method all databases must be deleted
	 */
//	@After
//	protected void afterAll() {
//		for(String name : getDatabaseNames())
//			if(databaseExists(name))
//				deleteDatabase(name);
//	}

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
	
	protected void clearTablesOfDatabase(String name) {
		for (Schema schema : getSchemas()) {
			for (Node node : getDataNodes()) {
				Schemas.emptyTables(schema, node.getUri());
			}
		}
	}
}
