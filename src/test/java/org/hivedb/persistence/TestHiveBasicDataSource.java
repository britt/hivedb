package org.hivedb.persistence;

import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.springframework.beans.BeanUtils;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

public class TestHiveBasicDataSource extends H2HiveTestCase{

	@Test
	public void testPoolSize() {
		HiveBasicDataSource ds = new HiveBasicDataSource(getConnectString("testDb"));
		assertEquals(HiveBasicDataSource.DEFAULT_POOL_SIZE, ds.getMaxActive());
		
		System.setProperty(HiveBasicDataSource.CONNECTION_POOL_SIZE, "20");
		HiveBasicDataSource ds20 = new HiveBasicDataSource(getConnectString("testDb"));
		assertEquals(20, ds20.getMaxActive());
	}
	
	@Test
	public void testCloning() throws Exception {
		HiveBasicDataSource ds = new HiveBasicDataSource(getConnectString(getHiveDatabaseName()));
		fiddleProperties(ds);
		HiveBasicDataSource clone = ds.clone();
		validateEquality(ds, clone);
	}
	
	private HiveBasicDataSource fiddleProperties(HiveBasicDataSource ds) {
		ds.setConnectionTimeout(42l);
		ds.setSocketTimeout(53l);
		ds.setInitialSize(14);
		ds.setMinIdle(5);
		ds.setMaxIdle(6);
		ds.setMaxActive(9);
		ds.setMaxWait(99l);
		ds.setMaxOpenPreparedStatements(22);
		ds.setDefaultTransactionIsolation(83);
		ds.setDefaultAutoCommit(!ds.getDefaultAutoCommit());
		ds.setPoolPreparedStatements(!ds.isPoolPreparedStatements());
		ds.setMinEvictableIdleTimeMillis(666l);
		ds.setNumTestsPerEvictionRun(43);
		ds.setPassword("a password");
		ds.setTestOnBorrow(!ds.getTestOnBorrow());
		ds.setTestOnReturn(!ds.getTestOnReturn());
		ds.setTestWhileIdle(!ds.getTestWhileIdle());
		ds.setTimeBetweenEvictionRunsMillis(345l);
		ds.setUrl(ds.getUrl());
		ds.setUsername("test");
		ds.setValidationQuery(ds.getValidationQuery());
		return ds;
	}
	
	private void validateEquality(HiveBasicDataSource ds1, HiveBasicDataSource ds2) throws Exception{
		assertEquals(ds1.getConnectionTimeout(), ds2.getConnectionTimeout());
		assertEquals(ds1.getSocketTimeout(), ds2.getSocketTimeout());
		assertEquals(ds1.getInitialSize(), ds2.getInitialSize());
		assertEquals(ds1.getMinIdle(), ds2.getMinIdle());
		assertEquals(ds1.getMaxIdle(), ds2.getMaxIdle());
		assertEquals(ds1.getMaxActive(), ds2.getMaxActive());
		assertEquals(ds1.getMaxWait(), ds2.getMaxWait());
		assertEquals(ds1.getMaxOpenPreparedStatements(), ds2.getMaxOpenPreparedStatements());
		assertEquals(ds1.getDefaultTransactionIsolation(), ds2.getDefaultTransactionIsolation());
		assertEquals(ds1.isPoolPreparedStatements(), ds2.isPoolPreparedStatements());
		assertEquals(ds1.getMinEvictableIdleTimeMillis(), ds2.getMinEvictableIdleTimeMillis());
		assertEquals(ds1.getNumTestsPerEvictionRun(), ds2.getNumTestsPerEvictionRun());
		assertEquals(ds1.getPassword(), ds2.getPassword());
		assertEquals(ds1.getTestOnBorrow(), ds2.getTestOnBorrow());
		assertEquals(ds1.getTestOnReturn(), ds2.getTestOnReturn());
		assertEquals(ds1.getTestWhileIdle(), ds2.getTestWhileIdle());
		assertEquals(ds1.getTimeBetweenEvictionRunsMillis(), ds2.getTimeBetweenEvictionRunsMillis());
		assertEquals(ds1.getUrl(), ds2.getUrl());
		assertEquals(ds1.getUsername(), ds2.getUsername());
		assertEquals(ds1.getValidationQuery(), ds2.getValidationQuery());
	}
}
