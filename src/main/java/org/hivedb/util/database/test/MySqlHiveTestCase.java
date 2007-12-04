package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class MySqlHiveTestCase extends MysqlTestCase {
	
	HiveTestCase hiveTestCase;
	public MySqlHiveTestCase() {
		hiveTestCase = new HiveTestCase(
			getEntityClasses(),
			HiveDbDialect.MySql, 
			new Unary<String,String>() {
				public String f(String databaseName) {
					return getConnectString(databaseName);
				}
			},
			getDataNodeNames());
		cleanupAfterEachTest = true;
	}

	protected List<Class<? extends Object>> getEntityClasses() {
		return Arrays.asList(getPartitionDimensionClass(), WeatherReport.class);
	}
	
	protected Class<?> getPartitionDimensionClass() {
		return Continent.class;
	}
	
	protected Collection<String> getDataNodeNames() {
		return Collections.emptyList();
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
	
	public Hive getHive() { 
		return hiveTestCase.getHive();
	}
	
	public EntityHiveConfig getEntityHiveConfig()
	{
		return hiveTestCase.getEntityHiveConfig();
	}
	
	protected String getHiveDatabaseName() {
		return "hive";
	}
	
	public Collection<String> getDatabaseNames() {
		return Transform.flatten(new Collection[] {
			Collections.singletonList(getHiveDatabaseName()),
			getDataNodeNames() });	
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
