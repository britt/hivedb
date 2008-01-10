package org.hivedb.services;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.Temperature;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataGenerationServiceTest extends H2TestCase {
	private static Hive hive;
	private EntityHiveConfig config;
	private HiveSessionFactory factory;

	@Test
	public void testGeneration() throws Exception {
		Collection<Class<?>> classes = Arrays.asList(getEntityClasses());
		DataGenerationService service = new DataGenerationServiceImpl(classes, hive);
		int partitionKeyCount = 2;
		int instanceCount = 4;
		
		Collection<Serializable> ids = service.generate(WeatherReport.class.getName(), partitionKeyCount, instanceCount);
		assertEquals(partitionKeyCount*instanceCount, ids.size());
		
		DataAccessObject<Object, Serializable> dao = 
			new BaseDataAccessObject(ConfigurationReader.readConfiguration(WeatherReport.class), hive, factory);
		
		Collection<Object> instances = Lists.newArrayList();
		for(Serializable id : ids) {
			Object instance = dao.get(id);
			assertNotNull(instance);
			instances.add(instance);
		}
		final EntityConfig entityConfig = config.getEntityConfig(WeatherReport.class);
		assertEquals(partitionKeyCount, Filter.grepUnique(new Unary<Object, Object>(){
			public Object f(Object item) {
				return entityConfig.getPrimaryIndexKey(item);
			}}, instances).size());
		assertEquals(partitionKeyCount*instanceCount, Filter.grepUnique(new Unary<Object, Object>(){
			public Object f(Object item) {
				return entityConfig.getId(item);
			}}, instances).size());
	}
	
	@BeforeMethod
	public void setup() throws Exception {
		super.beforeMethod();
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();	
		ConfigurationReader reader = new ConfigurationReader(getEntityClasses());
		reader.install(getConnectString(getHiveDatabaseName()));
		hive = getHive();
		for(String nodeName : getDataNodeNames())
			try {
				hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
				for(Schema s : getSchemata())
					s.install(getConnectString(nodeName));
			} catch (HiveReadOnlyException e) {
				throw new HiveRuntimeException("Hive was read-only", e);
			}
		config = reader.getHiveConfiguration();
		factory = getSessionFactory();
	}
	
	private Collection<Schema> getSchemata() {
		return Arrays.asList(new Schema[]{
				new WeatherSchema()
		});
	}
	
	private Class<?>[] getEntityClasses() {
		return new Class<?>[]{WeatherReport.class, WeatherEvent.class, Temperature.class};
	}

	@Override
	public Collection<String> getDatabaseNames() {
		Collection<String> dbs = Lists.newArrayList();
		dbs.addAll(getDataNodeNames());
		if(!dbs.contains(getHiveDatabaseName()))
			dbs.add(getHiveDatabaseName());
		return dbs;
	}

	private Collection<String> getDataNodeNames() {
		return Collections.singletonList(getHiveDatabaseName());
	}

	public String getHiveDatabaseName() {
		return "hive";
	}

	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
	
	private HiveSessionFactory getSessionFactory() {
		return new HiveSessionFactoryBuilderImpl(
				getConnectString(getHiveDatabaseName()), 
				Arrays.asList(getEntityClasses()), 
				new SequentialShardAccessStrategy());
	}
	
	
}
