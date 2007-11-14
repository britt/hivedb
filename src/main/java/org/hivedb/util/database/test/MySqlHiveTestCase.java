package org.hivedb.util.database.test;

import java.util.Collection;
import java.util.Collections;

import org.hivedb.HiveFacade;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class MySqlHiveTestCase extends MysqlTestCase {
	
	HiveTestCase hiveTestCase;
	public MySqlHiveTestCase() {
		hiveTestCase = new HiveTestCase(HiveDbDialect.MySql, new Unary<String,String>() {
			public String f(String databaseName) {
				return getConnectString(databaseName);
			}
		});
		cleanupAfterEachTest = true;
	}
	
	@Override
	@BeforeClass
	protected void beforeClass() {
		super.beforeClass();
		hiveTestCase.beforeClass();
		super.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		hiveTestCase.beforeMethod();
	}
	
	public HiveFacade getHive() { 
		return hiveTestCase.getHive();
	}
	
	public EntityHiveConfig getEntityHiveConfig()
	{
		return hiveTestCase.getEntityHiveConfig();
	}
	
	protected String getHiveDatabaseName() {
		return "hive";
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Collections.singletonList(getHiveDatabaseName());  
	}

	protected Collection<Resource> createResources() {
		return hiveTestCase.createResources();
	}
	protected Resource createResource() {
		return hiveTestCase.createResource();
	}
	protected SecondaryIndex createSecondaryIndex() {
		return hiveTestCase.createSecondaryIndex();
	}
	protected SecondaryIndex createSecondaryIndex(int id) {
		return hiveTestCase.createSecondaryIndex(id);
	}
	protected Node createNode(String name) {
		return hiveTestCase.createNode(name);
	}
	protected PartitionDimension createPopulatedPartitionDimension() {
		return hiveTestCase.createPopulatedPartitionDimension();
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return hiveTestCase.createEmptyPartitionDimension();
	}
	protected String partitionDimensionName() {
		return hiveTestCase.partitionDimensionName();
	}
	protected HiveSemaphore createHiveSemaphore() {
		return hiveTestCase.createHiveSemaphore();
	}
}
