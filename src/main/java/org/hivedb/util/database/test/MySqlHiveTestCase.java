package org.hivedb.util.database.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObjectFactory;
import org.hivedb.hibernate.DataAccessObject;
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
import org.testng.annotations.BeforeSuite;

public class MySqlHiveTestCase extends MysqlTestCase {
	
	@BeforeClass
	protected void beforeClass() {
		cleanupAfterEachTest = true;
		// Each test class may have its own settings for the HiveTestCase
		hiveTestCase = createHiveTestCase();
		hiveTestCase.beforeClass();
	}
	
	@Override
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		hiveTestCase.beforeMethod();
		installDataSchemas();
	}
	private HiveTestCase createHiveTestCase() {
		return new HiveTestCase(
				getHiveDatabaseName(),
				getMappedClasses(),
				HiveDbDialect.MySql, 
				new Unary<String,String>() {
					public String f(String databaseName) {
						return getConnectString(databaseName);
					}
				},
				getDataNodeNames(),
				cleanupAfterEachTest || createDatabaseIfNoCleanup);
	}
	
	protected HiveTestCase hiveTestCase;


	protected Collection<Class<?>> getMappedClasses() {
		return Arrays.asList(getPartitionDimensionClass(), WeatherReport.class, WeatherEvent.class);
	}
	protected Class<?> getPartitionDimensionClass() {
		return Continent.class;
	}
	
	protected Collection<String> getDataNodeNames() {
		return Arrays.asList("data1", "data2");
	}
	
	protected void installDataSchemas() {
		for (Schema schema : getDataNodeSchemas()) {
			for (Node node : hiveTestCase.getOrLoadHive().getNodes()) {
				schema.install(node);
			}
		}
	}
	
	protected Collection<Schema> getSchemas() {
		return Transform.flatten(hiveTestCase.getHiveSchemas(), getDataNodeSchemas());
	}
	
	// Gets Schema instances for each entity on each data node
	protected Collection<Schema> getDataNodeSchemas() {
		return Arrays.asList(new Schema[] { ContinentalSchema.getInstance(), WeatherSchema.getInstance() });
	}
	
	public Collection<String> getDatabaseNames() {
		return Transform.flatten(new Collection[] {
			Collections.singletonList(getHiveDatabaseName()),
			getDataNodeNames() });	
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
	public DataAccessObject<? extends Object, ? extends Serializable> getDao(Class clazz) {	
		return new BaseDataAccessObjectFactory<Object,Serializable>(
				getEntityHiveConfig(),
				getMappedClasses(),
				clazz,
				getHive()).create();
	}
	
	@Override
	protected Collection<Node> getDataNodes() {
		return hiveTestCase.getOrLoadHive().getNodes();
	}	
}
