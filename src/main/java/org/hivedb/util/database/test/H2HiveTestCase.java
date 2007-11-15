package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.Continent;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.hibernate.WeatherReportImpl;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class H2HiveTestCase extends H2TestCase {
	
	HiveTestCase hiveTestCase;
	public H2HiveTestCase() {
		hiveTestCase = new HiveTestCase(
			getPartitionDimensionClass(),
			getEntityClasses(),
			HiveDbDialect.H2, 
			new Unary<String,String>() {
				public String f(String databaseName) {
					return getConnectString(databaseName);
				}
			});
		cleanupAfterEachTest = true;
	}

	protected List<Class<? extends Object>> getEntityClasses() {
		return Arrays.asList(getPartitionDimensionClass(), WeatherReportImpl.class);
	}
	protected Class<?> getPartitionDimensionClass() {
		return Continent.class;
	}
	
	protected HiveSessionFactoryBuilderImpl getFactory() {
		return new HiveSessionFactoryBuilderImpl(
				getEntityHiveConfig(), new SequentialShardAccessStrategy());
	}
	
	@Override
	@BeforeClass
	protected void beforeClass() {
		hiveTestCase.beforeClass();
		super.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		hiveTestCase.beforeMethod();
	}
	
	public Hive getHive() { 
		return hiveTestCase.getHive();
	}
	
	public EntityHiveConfig getEntityHiveConfig()
	{
		return hiveTestCase.getEntityHiveConfig();
	}
	
	public String getHiveDatabaseName() {
		return hiveTestCase.getHiveDatabaseName();
	}
	public Collection<String> getDatabaseNames() {
		return hiveTestCase.getDatabaseNames();  
	}
	
	// Sample data
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
