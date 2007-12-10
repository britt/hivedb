package org.hivedb.util.database.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.serialization.BlobbedEntity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class H2HiveTestCase extends H2TestCase {
	
	@Override
	@BeforeClass
	protected void beforeClass() {
		cleanupAfterEachTest = true;
		hiveTestCase = createHiveTestCase();
		hiveTestCase.beforeClass();
		super.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		hiveTestCase = createHiveTestCase();
		hiveTestCase.beforeMethod();
		installDataSchemas();
	}

	private HiveTestCase createHiveTestCase() {
		return new HiveTestCase(
				getEntityClasses(),
				getNonEntityClasses(),
				HiveDbDialect.H2, 
				new Unary<String,String>() {
					public String f(String databaseName) {
						return getConnectString(databaseName);
					}
				},
				getDataNodeNames());
	}
	
	protected HiveTestCase hiveTestCase;


	@SuppressWarnings("unchecked")
	protected Collection<Class<?>> getEntityClasses() {
		return Arrays.asList(
				getPartitionDimensionClass(),
				WeatherReport.class, 
				WeatherEvent.class);
	}
	protected Class<?> getPartitionDimensionClass() {
		return Continent.class;
	}
	protected Collection<Class> getNonEntityClasses() {
		return Arrays.asList((Class)BlobbedEntity.class);
	}
	
	protected Collection<String> getDataNodeNames() {
		return Arrays.asList("data1", "data2");
	}
	
	protected void installDataSchemas() {
		for (String dataNodeName : getDataNodeNames()) {
			new ContinentalSchema(getConnectString(dataNodeName)).install();
			new WeatherSchema(getConnectString(dataNodeName)).install();
		}
	}
	
	@SuppressWarnings("unchecked")
	public Collection<String> getDatabaseNames() {
		return Transform.flatten(new Collection[] {
			Collections.singletonList(getHiveDatabaseName()),
			getDataNodeNames() });	
	}
	
	protected HiveSessionFactoryBuilderImpl getFactory() {
		return new HiveSessionFactoryBuilderImpl(getEntityHiveConfig(), getHive(), new SequentialShardAccessStrategy());
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
