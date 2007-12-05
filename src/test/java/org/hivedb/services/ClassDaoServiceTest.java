package org.hivedb.services;

import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObjectFactory;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilder;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.hibernate.annotations.AnnotationHelper;
import org.hivedb.hibernate.annotations.Index;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.util.BeanGenerator;
import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import sun.util.calendar.BaseCalendar;

public class ClassDaoServiceTest extends H2TestCase {
	private static int INSTANCE_COUNT = 5;
	private EntityHiveConfig config;
	private Hive hive;
	private static Delay<ClassDaoService>[] services;
	private HiveSessionFactory factory;
	
	@SuppressWarnings("unchecked")
	@BeforeClass
	public void initializeDataProvider() {
		services = new Delay[]{
			new LazyInitializer(
					new Delay<ClassDaoService>(){
						public ClassDaoService f() {
							return new BaseClassDaoService(
									ConfigurationReader.readConfiguration(WeatherReportImpl.class),
									new BaseDataAccessObject(
											WeatherReportImpl.class,
											config,
											hive,
											factory));}})	
		};
	}
	
	@SuppressWarnings("unused")
	@DataProvider(name = "service")
	private Iterator<?> getServices() {
		return new ServiceIterator();
	}
	
	@Override
	protected void beforeMethod() {
		super.beforeMethod();
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();		
		ConfigurationReader reader = new ConfigurationReader(getEntityClasses());
		reader.install(getConnectString(getHiveDatabaseName()));
		hive = getHive();
		for(String nodeName : getDataNodeNames())
			try {
				hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
				new WeatherSchema(getConnectString(nodeName)).install();
			} catch (HiveReadOnlyException e) {
				throw new HiveRuntimeException("Hive was read-only", e);
			}
		config = reader.getHiveConfiguration(hive);
		factory = getSessionFactory();
	}
	
	@SuppressWarnings("deprecation")
	protected Class<?>[] getEntityClasses() {
		return new Class<?>[]{WeatherReportImpl.class};
	}

	@Test(dataProvider = "service")
	public void saveAndRetrieveInstance(ClassDaoService service) throws Exception {
		ServiceResponse original = getPersistentInstance(service);
		ServiceResponse response = service.get(getId(Atom.getFirst(original.getInstances())));
		validateRetrieval(original, response);
	}

	@Test(dataProvider = "service")
	public void retrieveInstancesUsingAllIndexedProperties(ClassDaoService service) throws Exception {
		ServiceResponse original = getPersistentInstance(service);
		List<Method> indexes = AnnotationHelper.getAllMethodsWithAnnotation(original.getClass(), Index.class);
		
		for(Method index : indexes) {
			String indexPropertyName = BeanUtils.findPropertyForMethod(index).getName();
			ServiceResponse response = service.getByReference(indexPropertyName, ReflectionTools.invokeGetter(original, indexPropertyName));
			validateRetrieval(original, response);
		}
	}
	
	@Test(dataProvider = "service")
	public void notDetectTheExistenceOfNonPersistentEntities(ClassDaoService service) throws Exception {
		assertFalse(service.exists(getId(getInstance(service.getPersistedClass()))));
	}
	
	@Test(dataProvider = "service")
	public void detectTheExistenceOfPersistentEntities(ClassDaoService service) throws Exception {
		assertTrue(service.exists(getId(Atom.getFirst(getPersistentInstance(service).getInstances()))));
	}
	
	@Test(dataProvider = "service")
	public void saveMultipleInstances(ClassDaoService service) throws Exception {
		List<Object> instances = Lists.newArrayList();
		for(int i=0; i<INSTANCE_COUNT; i++)
			instances.add(getInstance(service.getPersistedClass()));
		service.saveAll(instances);
		for(Object instance : instances)
			validateRetrieval(
					new ServiceResponseImpl(config.getEntityConfig(instance.getClass()), Collections.singletonList(instance)), 
					service.get(getId(instance)));
	}
	
	@Test(dataProvider = "service")
	public void deleteAnInstance(ClassDaoService service) throws Exception {
		ServiceResponse deleted = getPersistentInstance(service);
		service.delete(getId(Atom.getFirst(deleted.getInstances())));
		assertFalse(service.exists(getId(Atom.getFirst(deleted.getInstances()))));
	}
	
	private Object getInstance(Class<Object> clazz) throws Exception {
		return new BeanGenerator<Object>(clazz).generate();
	}
	
	private ServiceResponse getPersistentInstance(ClassDaoService service) throws Exception {
		return service.save(getInstance(service.getPersistedClass()));
	}
	
	private Serializable getId(Object instance) {
		return config.getEntityConfig(instance.getClass()).getId(instance);
	}
	
	private void validateRetrieval(ServiceResponse original, ServiceResponse response) {
		assertEquals("Expected only one instance", 1, response.getInstances().size());
		validate(original, response);
	}
	
	private void validate(ServiceResponse expected, ServiceResponse actual) {
		assertEquals(expected.getInstances().size(), actual.getInstances().size());
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

	private String getHiveDatabaseName() {
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
	
	private class ServiceIterator implements Iterator<Object[]> {
		int idx = 0;
		public boolean hasNext() {
			return services.length != 0 && idx < services.length;
		}

		public Object[] next() {
			Object[] objects = new Object[]{services[idx].f()};
			idx++;
			return objects;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	public class LazyInitializer implements Delay<Object> {
		private Delay<?> delay;
		public LazyInitializer(Delay<?> delay) {this.delay = delay;}
		public Object f() {
			beforeMethod();
			return delay.f();
		}
		
	}
}
