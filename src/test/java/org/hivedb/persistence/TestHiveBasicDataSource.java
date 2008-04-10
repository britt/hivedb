package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;

import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.test.ClassNameContextLoader;
import org.hivedb.test.ContextAwareTest;
import org.hivedb.test.DatabaseInitializer;
import org.hivedb.test.TestNGTools;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@ContextConfiguration(locations="single.db.xml",loader=ClassNameContextLoader.class)
public class TestHiveBasicDataSource extends ContextAwareTest{
	public DatabaseInitializer db;
	
	@Test(dataProvider="dbs", groups="database")
	public void testPoolSize(String name) {
		HiveBasicDataSource ds = new HiveBasicDataSource(db.getConnectString(name));
		assertEquals(HiveBasicDataSource.DEFAULT_POOL_SIZE, ds.getMaxActive());
		
		System.setProperty(HiveBasicDataSource.CONNECTION_POOL_SIZE, "20");
		HiveBasicDataSource ds20 = new HiveBasicDataSource(db.getConnectString(name));
		assertEquals(20, ds20.getMaxActive());
	}
	
	@Test(dataProvider="dbs", groups="database")
	public void testCloning(String name) throws Exception {
		HiveBasicDataSource ds = new HiveBasicDataSource(db.getConnectString(name));
		fiddleProperties(ds);
		HiveBasicDataSource clone = ds.clone();
		validateEquality(ds, clone);
	}
	
	@Test(dataProvider="dbs", groups="database")
	public void testConnection(String name) throws Exception {
		HiveBasicDataSource ds = new HiveBasicDataSource(db.getConnectString(name));
		Connection connection = ds.getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement("select 1");
		preparedStatement.execute();
		ResultSet resultSet = preparedStatement.getResultSet();
		resultSet.next();
		assertEquals(new Integer(1), resultSet.getObject(1));
		resultSet.close();
		preparedStatement.close();
		connection.close();
	}
	
	@SuppressWarnings("unused")
	@DataProvider(name="dbs")
	private Iterator<Object[]> getNames() {
		return TestNGTools.makeObjectArrayIterator(db.getDatabaseNames());
	}
	
	private HiveBasicDataSource fiddleProperties(HiveBasicDataSource ds) {
		ds.setMaxActive(9);
		ds.setPassword("a password");
		ds.setUrl(ds.getUrl());
		ds.setUsername("test");
		ds.setValidationQuery(ds.getValidationQuery());
		return ds;
	}
	
	private void validateEquality(HiveBasicDataSource ds1, HiveBasicDataSource ds2) throws Exception{
		assertEquals(ds1.getMaxActive(), ds2.getMaxActive());
		assertEquals(ds1.getPassword(), ds2.getPassword());
		assertEquals(ds1.getUrl(), ds2.getUrl());
		assertEquals(ds1.getUsername(), ds2.getUsername());
		assertEquals(ds1.getValidationQuery(), ds2.getValidationQuery());
	}

	public DatabaseInitializer getDb() {
		return db;
	}

	public void setDb(DatabaseInitializer db) {
		this.db = db;
	}
}
