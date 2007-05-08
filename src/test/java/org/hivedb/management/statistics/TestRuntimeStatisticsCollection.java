package org.hivedb.management.statistics;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.IndexSchema;
import org.hivedb.util.database.DaoTestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRuntimeStatisticsCollection extends DaoTestCase{
	private Hive hive;
	private ApplicationContext context;
	
	@BeforeClass
	public void setup() throws Exception {
		new HiveInstaller(getConnectString()).run();
		
		String[] configFiles = new String[] {"applicationContext-test.xml", "hivedb-jmx-mbeans.xml", "hivedb-jmx.xml"};
		context = new ClassPathXmlApplicationContext(configFiles);
		
		hive = (Hive) context.getBean("hive");
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		new IndexSchema(hive.getPartitionDimension(partitionDimensionName())).install();
		hive.addNode(hive.getPartitionDimension(partitionDimensionName()), createNode());
		hive.insertPrimaryIndexKey(partitionDimensionName(), intKey());
	}
	
	@Test
	public void testWriteConnectionTracking() throws Exception{
		Collection<Connection> connections = new ArrayList<Connection>();
		
		for(int i=0; i<5; i++)
			connections.add( hive.getConnection( hive.getPartitionDimension(partitionDimensionName()), intKey(), AccessType.ReadWrite));
		
		Assert.assertEquals(connections.size(), ((HivePerformanceStatistics) context.getBean("hiveStatistics")).getSumNewWriteConnections());
	}
	
	
	@Test
	public void testReadConnectionTracking() throws Exception{
		Collection<Connection> connections = new ArrayList<Connection>();
		for(int i=0; i<5; i++)
			connections.add( hive.getConnection( hive.getPartitionDimension(partitionDimensionName()), intKey(), AccessType.Read));
		
		Assert.assertEquals(connections.size(), ((HivePerformanceStatistics) context.getBean("hiveStatistics")).getSumNewReadConnections());
	}
	
	@Test
	public void testConnectionFailures() throws Exception {
		hive.updateHiveReadOnly(true);
		Collection<Connection> connections = new ArrayList<Connection>();
		for(int i=0; i<5; i++){
			try {
				connections.add( hive.getConnection( hive.getPartitionDimension(partitionDimensionName()), intKey(), AccessType.ReadWrite));
			} catch( Exception e) {
				//CRUSH! KILL! DESTROY!
			}
		}
		
		Assert.assertEquals(5, ((HivePerformanceStatistics) context.getBean("hiveStatistics")).getSumConnectionFailures());
	}
	
	
	@AfterMethod
	public void resetReadOnly() throws HiveException {
		hive.updateHiveReadOnly(false);
	}
	
	@Override
	public String getDatabaseName() {
		return "storage_test";
	}

	public Integer intKey() { return new Integer(7); }
}
