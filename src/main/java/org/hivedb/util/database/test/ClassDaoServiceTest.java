package org.hivedb.util.database.test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.Index;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.services.BaseClassDaoService;
import org.hivedb.services.ClassDaoService;
import org.hivedb.services.ServiceContainer;
import org.hivedb.services.ServiceResponse;
import org.hivedb.services.ServiceResponseImpl;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClassDaoServiceTest extends H2TestCase {
	public static int INSTANCE_COUNT = 5;
	protected EntityHiveConfig config;
	protected Hive hive;
	protected static Collection<LazyInitializer> services = Lists.newArrayList();
	protected static Collection<Class> entityClasses = Lists.newArrayList();
	protected HiveSessionFactory factory;
	
	public static void addEntity(Class clazz, Schema schema) {
		addEntity(clazz, new LazyInitializer(clazz, schema));
	}
	
	public static void addEntity(Class clazz, LazyInitializer initializer) {
		entityClasses.add(clazz);
		services.add(initializer);
	}
	
	public static void clearEntities() {
		entityClasses.clear();
		services.clear();
	}
	
	@SuppressWarnings("unchecked")
	@BeforeClass
	public void initializeDataProvider() {
	}
	
	@SuppressWarnings("unused")
	@DataProvider(name = "service")
	protected Iterator<?> getServices() {
		return new ServiceIterator(services.iterator(),this);
	}
	
	@SuppressWarnings("deprecation")
	protected Class<?>[] getEntityClasses() {
		return (Class<?>[]) entityClasses.toArray(new Class<?>[]{});
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
		assertFalse(service.exists(getId(getInstance(toClass(service.getPersistedClass())))));
	}
	
	@Test(dataProvider = "service")
	public void detectTheExistenceOfPersistentEntities(ClassDaoService service) throws Exception {
		assertTrue(service.exists(getId(Atom.getFirst(getPersistentInstance(service).getInstances()))));
	}
	
	@Test(dataProvider = "service")
	public void saveMultipleInstances(ClassDaoService service) throws Exception {
		List<Object> instances = Lists.newArrayList();
		for(int i=0; i<INSTANCE_COUNT; i++)
			instances.add(getInstance(toClass(service.getPersistedClass())));
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
	
	protected Object getInstance(Class<Object> clazz) throws Exception {
		return new GenerateInstance<Object>(clazz).generate();
	}
	
	protected ServiceResponse getPersistentInstance(ClassDaoService service) throws Exception {
		return service.save(getInstance(toClass(service.getPersistedClass())));
	}
	
	protected Serializable getId(Object instance) {
		return config.getEntityConfig(instance.getClass()).getId(instance);
	}
	
	protected void validateRetrieval(ServiceResponse original, ServiceResponse response) {
		assertEquals(original.getInstances().size(), response.getInstances().size());
		validate(original, response);
	}
	
	protected void validate(ServiceResponse expected, ServiceResponse actual) {
		assertEquals(expected.getInstances().size(), actual.getInstances().size());
		Map<Integer, ServiceContainer> expectedMap = getInstanceHashCodeMap(expected);
		Map<Integer, ServiceContainer> actualMap = getInstanceHashCodeMap(actual);
		
		for(Integer key : actualMap.keySet()) {
			assertTrue(
					String.format("Expected results did not contian a ServiceContainer with hashCode %s", key), 
					expectedMap.containsKey(key));
			validate(expectedMap.get(key), actualMap.get(key));
		}
	}

	private Map<Integer, ServiceContainer> getInstanceHashCodeMap(
			ServiceResponse expected) {
		return Transform.toMap(new Unary<ServiceContainer, Integer>(){
			public Integer f(ServiceContainer item) {
				return item.getInstance().hashCode();
			}}, new Transform.IdentityFunction<ServiceContainer>(), expected.getContainers());
	}
	
	private void validate(ServiceContainer expected, ServiceContainer actual) {
		assertEquals(expected.getVersion(), actual.getVersion());
		assertEquals(expected.getInstance(), actual.getInstance());
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
	
	public static class LazyInitializer implements Delay<Object> {
		private Delay<?> delay;
		private Schema schema;
		private ClassDaoServiceTest test;
		public LazyInitializer(Delay<?> delay, Schema schema) {
			this.delay = delay;
			this.schema = schema;
		}
		public LazyInitializer(final Class clazz, Schema schema){
			this.schema = schema;
			this.delay = new Delay<ClassDaoService>(){
				public ClassDaoService f() {
					return new BaseClassDaoService(
							ConfigurationReader.readConfiguration(clazz),
							new BaseDataAccessObject(
									test.config.getEntityConfig(clazz),
									test.hive,
									test.factory));
				}};
		}
		public Object f() {
			initialize(schema, test);
			return delay.f();
		}
		public LazyInitializer setTest(ClassDaoServiceTest test) {this.test = test; return this;}
	}
	
	private class ServiceIterator implements Iterator<Object[]> {
		Iterator<LazyInitializer> i;
		ClassDaoServiceTest test;
		public ServiceIterator(Iterator<LazyInitializer> i, ClassDaoServiceTest test) { 
			this.i = i;
			this.test = test;
		}
		
		public Object[] next() {return new Object[]{
				i.next().setTest(test).f()};
		}
		public void remove() {throw new UnsupportedOperationException();}
		public boolean hasNext() {return i.hasNext();}
		
	}
	
	@Override
	protected void beforeMethod() {
	}

	protected void superClassBeforeMethod() {
		super.beforeMethod();
	}
	
	protected static void initialize(Schema schema, ClassDaoServiceTest test) {
		test.superClassBeforeMethod();
		new HiveInstaller(test.getConnectString(test.getHiveDatabaseName())).run();		
		ConfigurationReader reader = new ConfigurationReader(test.getEntityClasses());
		reader.install(test.getConnectString(test.getHiveDatabaseName()));
		test.hive = test.getHive();
		for(String nodeName : test.getDataNodeNames())
			try {
				test.hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
				schema.install(test.getConnectString(nodeName));
			} catch (HiveReadOnlyException e) {
				throw new HiveRuntimeException("Hive was read-only", e);
			}
		test.config = reader.getHiveConfiguration();
		test.factory = test.getSessionFactory();
	}
	
	protected Class<Object> toClass(String className) {
		try {
			return (Class<Object>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
