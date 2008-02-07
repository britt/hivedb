package org.hivedb.util.database.test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.IndexParam;
import org.hivedb.annotations.Validate;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.services.BaseClassDaoService;
import org.hivedb.services.ClassDaoService;
import org.hivedb.services.ServiceResponse;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.ReflectionTools.DiffCollection;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Toss;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.validators.NoValidator;
import org.springframework.beans.BeanUtils;
import org.testng.Assert;
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
		addEntity(clazz, new LazyInitializer(clazz, Lists.newList(schema)));
	}
	public static void addEntity(Class clazz, Collection<Schema> schemaList) {
		addEntity(clazz, new LazyInitializer(clazz, schemaList));
	}
	
	public static void addEntity(Class clazz) {
		entityClasses.add(clazz);
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
		Object original = getPersistentInstance(service);
		Object response = service.get(getId(original));
		validateRetrieval(original, response);
	}
	
	@Test(dataProvider = "service")
	public void saveAndRetrieveInstanceWithNullProperties(final ClassDaoService service) throws Exception {
		AssertUtils.assertThrows(new Toss() {
			public void f() throws Exception {
				Object original = getPersistentInstanceWithNullProperties(service);	
			}
		}, HiveRuntimeException.class);
	}
	
//	@Test(dataProvider = "service")
//	public void saveAndUpdateToEmptyCollectionProperties(final ClassDaoService service) throws Exception {
//		Object original = getPersistentInstance(service);
//		final EntityConfig entityConfig = config.getEntityConfig(toClass(service.getPersistedClass()));
//		Object update = new GenerateInstance<Object>((Class<Object>) entityConfig.getRepresentedInterface()).generateAndCopyProperties(original);
//		for (Method getter : ReflectionTools.getCollectionGetters(entityConfig.getRepresentedInterface())) {
//			String property = ReflectionTools.getPropertyNameOfAccessor(getter);
//			GeneratedInstanceInterceptor.setProperty(update, property, Collections.emptyList());
//		}
//		save(service, update);
//		Assert.assertEquals(service.get(getId(original)), update);
//		
//	}

	protected Object save(final ClassDaoService service, Object update) {
		return service.save(update);
	}
	protected Collection saveAll(ClassDaoService service, Collection<? extends Object> instances) {
		return service.saveAll(instances);
	}
	
	@Test(dataProvider = "service")
	public void saveAndRetrieveInstanceWithAllowedNullProperties(final ClassDaoService service) throws Exception {
		final Class<Object> clazz = (Class<Object>) Class.forName(service.getPersistedClass());
		Object instance = getInstance(clazz);
		for (Method getter : ReflectionTools.getGetters(clazz))
		{
			final String property = ReflectionTools.getPropertyNameOfAccessor(getter);
			final Validate annotation = (Validate)AnnotationHelper.getAnnotationDeeply(clazz, property, Validate.class);
			if (annotation != null && Class.forName(annotation.value()).equals(NoValidator.class) && !getter.getReturnType().isAssignableFrom(Collection.class))
			{
				ReflectionTools.invokeSetter(instance, property, null);
			}
		}
		save(service, instance);
		Object actual = service.get(config.getEntityConfig(clazz).getId(instance));
		for (Method getter : ReflectionTools.getGetters(clazz))
		{
			if(!getter.getReturnType().isAssignableFrom(Collection.class))
				Assert.assertEquals(getter.invoke(instance), getter.invoke(actual));
		}
	}

	@Test(dataProvider = "service")
	public void retrieveInstancesUsingAllIndexedProperties(final ClassDaoService service) throws Exception {
		final Object original = getPersistentInstance(service);
		if (original != null) {
			final EntityConfig entityConfig = config.getEntityConfig(toClass(service.getPersistedClass()));			
			for(EntityIndexConfig entityIndexConfig : entityConfig.getEntityIndexConfigs()) {
				final String indexPropertyName = entityIndexConfig.getPropertyName();
				// test retrieval with the single value or each item of the list of values
				for (Object value : entityIndexConfig.getIndexValues(original)) {		
					Object response = service.getByReference(indexPropertyName, value);
					validateRetrieval(Collections.singletonList(original), response);
				}
			}
		}
	}
	
	@Test(dataProvider = "service")
	public void retrieveInstancesUsingSubclassFinders(ClassDaoService service) throws Exception {
		if (service.getClass().equals(BaseClassDaoService.class))
			return;
		final Object original = getPersistentInstance(service);
		// Any method returning a ServiceResponse will be tested
		Collection<Method> finders =
			Transform.map(new Unary<Method, Method>() {
				public Method f(Method method) {
					return ReflectionTools.getMethodOfOwner(method);
				}},
				Filter.grep(new Predicate<Method>() {
					public boolean f(Method method) {
						return method.getReturnType().equals(ServiceResponse.class);
					}},
					ReflectionTools.getOwnedMethods(service.getClass())));
		final Object storedInstance = original;
		final EntityConfig entityConfig = config.getEntityConfig(storedInstance.getClass());
		for(final Method finder : finders) {
			Collection<Object> argumentValues = Transform.map(new Unary<Annotation[], Object>() {
				public Object f(Annotation[] annotations) {
					final Annotation annotation = Filter.grepSingleOrNull(
						new Predicate<Annotation>() {
							public boolean f(Annotation annotation) {
								return annotation.annotationType().equals(IndexParam.class);
							}
						},
						Arrays.asList(annotations));
					if (annotation == null) {
						throw new RuntimeException(
							String.format("Cannot resolve the index of parameter(s) of method %s, add an IndexParam annotation to each parameter", finder.getName()));
					}
					EntityIndexConfig entityIndexConfig = Atom.getFirstOrNull(Filter.grep(new Predicate<EntityIndexConfig>() {
						public boolean f(EntityIndexConfig entityIndexConfig) {
							return entityIndexConfig.getIndexName().equals(((IndexParam)annotation).value());
						}}, entityConfig.getEntityIndexConfigs()));
					// For EntityIndexConfig indexes
					if (entityIndexConfig != null)
						return Atom.getFirstOrThrow(entityIndexConfig.getIndexValues(storedInstance));
					// For DataIndex indexes
					else
						return ReflectionTools.invokeGetter(storedInstance, ((IndexParam)annotation).value());
				}},
				(Iterable<? extends Annotation[]>)Arrays.asList(finder.getParameterAnnotations()));
			ServiceResponse response = (ServiceResponse) finder.invoke(service, argumentValues.toArray());
			validateRetrieval(original, response);
		}
	}
	
	@Test(dataProvider = "service")
	public void notDetectTheExistenceOfNonPersistentEntities(ClassDaoService service) throws Exception {
		assertFalse(service.exists(getId(getInstance(toClass(service.getPersistedClass())))));
	}
	
	@Test(dataProvider = "service")
	public void detectTheExistenceOfPersistentEntities(ClassDaoService service) throws Exception {
		assertTrue(service.exists(getId(getPersistentInstance(service))));
	}
	
	@Test(dataProvider = "service")
	public void saveMultipleInstances(ClassDaoService service) throws Exception {
		List<Object> instances = Lists.newArrayList();
		for(int i=0; i<INSTANCE_COUNT; i++)
			instances.add(getInstance(toClass(service.getPersistedClass())));
		saveAll(service, instances);
		for(Object instance : instances)
			validateRetrieval(
					instance, 
					service.get(getId(instance)));
	}

	
	
//	@Test(dataProvider = "service")
//	public void testUpdateComplexCollectionItems(ClassDaoService service) throws Exception {
//		Object original = getPersistentInstance(service);
//		final EntityConfig entityConfig = config.getEntityConfig(toClass(service.getPersistedClass()));
//		Object updated = service.get(entityConfig.getId(original));
//		for (EntityIndexConfig entityIndexConfig : entityConfig.getEntityIndexConfigs()) {
//			final String propertyName = entityIndexConfig.getPropertyName();
//			if (ReflectionTools.isComplexCollectionItemProperty(entityConfig.getRepresentedInterface(), propertyName)) {
//				// Test collection item updates
//				List<Object> updatedItems = new ArrayList<Object>((Collection<Object>)ReflectionTools.invokeGetter(updated, propertyName));
//				// Delete the first
//				updatedItems.remove(0);
//				// Update the second
//				final Object updateItem = updatedItems.get(0);
//				Collection<String> simpleProperties = ReflectionTools.getPropertiesOfPrimitiveGetters(updateItem.getClass());
//				
//				Object generatedPrimitiveValue=null;
//				if (simpleProperties.size() > 0) {
//					final String firstProperty = Atom.getFirst(
//							Filter.grepFalseAgainstList(Collections.singleton(entityIndexConfig.getInnerClassPropertyName()), simpleProperties));
//					generatedPrimitiveValue = new GeneratePrimitiveValue<Object>(
//							(Class<Object>) ReflectionTools.getPropertyType(updateItem.getClass(), firstProperty)).generate();
//					ReflectionTools.invokeSetter(updateItem, firstProperty, generatedPrimitiveValue);
//				}
//				// Add a third
//				updatedItems.add(new GenerateInstance<Object>((Class<Object>) updateItem.getClass()).generate());
//				
//				GeneratedInstanceInterceptor.setProperty(updated, propertyName, updatedItems);
//				save(service, updated);
//				final Object persisted = service.get(entityConfig.getId(updated));
//				assertFalse(updated.equals(original));
//				// Check the updated collection
//					// size should be equal
//				final Collection<Object> persistedItems = new ArrayList<Object>((Collection<Object>)ReflectionTools.invokeGetter(persisted, propertyName));
//				assertEquals(entityIndexConfig.getIndexValues(updated).size(), persistedItems.size());
//					// first item should be removed
//				final Collection<Object> originalItems = new ArrayList<Object>((Collection<Object>)ReflectionTools.invokeGetter(original, propertyName));
//				assertFalse(Filter.grepItemAgainstList(Atom.getFirst(originalItems), persistedItems));
//					// should be an updated item 
//				if (simpleProperties.size() > 0) {
//					final String firstProperty = Atom.getFirst(simpleProperties);
//					
//					assertTrue(Filter.grepItemAgainstList(generatedPrimitiveValue,
//						Transform.map(new Unary<Object,Object>() {
//							public Object f(Object item) {
//								return ReflectionTools.invokeGetter(item, firstProperty);
//						}}, persistedItems)));
//				}
//					// new item should exist
//				assertTrue(Filter.grepItemAgainstList(Atom.getLast(updatedItems), persistedItems));
//			}
//		}
//	
//	}
	
	@Test(dataProvider = "service")
	public void deleteAnInstance(ClassDaoService service) throws Exception {
		Object deleted = getPersistentInstance(service);
		service.delete(getId(deleted));
		assertFalse(service.exists(getId(deleted)));
	}
	
	protected Object getInstance(Class<Object> clazz) throws Exception {
		return new GenerateInstance<Object>(clazz).generate();
	}
	protected Object getPersistentInstance(ClassDaoService service) throws Exception {
		return save(service, getInstance(toClass(service.getPersistedClass())));
	}
	
	protected Object getInstanceWithNullProperties(Class<Object> clazz) throws Exception {
		Object instance = new GenerateInstance<Object>(clazz).generate();
		for (Method getter : ReflectionTools.getComplexGetters(clazz))
			GeneratedInstanceInterceptor.setProperty(
					instance,
					ReflectionTools.getPropertyNameOfAccessor(getter),
					null);
		return instance;
	}
	protected Object getPersistentInstanceWithNullProperties(ClassDaoService service) throws Exception {
		return save(service, getInstanceWithNullProperties(toClass(service.getPersistedClass())));
	}
	
	
	protected Serializable getId(Object instance) {
		return config.getEntityConfig(instance.getClass()).getId(instance);
	}
	
	protected void validateRetrieval(Object original, Object response) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Assert.assertEquals(response.hashCode(), original.hashCode());
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
		private Collection<Schema> schemaList;
		private ClassDaoServiceTest test;
		public LazyInitializer(Delay<?> delay, Schema schema) {
			this.delay = delay;
			this.schemaList = Collections.singletonList(schema);
		}
		public LazyInitializer(Delay<?> delay, Collection<Schema> schemaList) {
			this.delay = delay;
			this.schemaList = schemaList;
		}
		public LazyInitializer(final Class clazz, Collection<Schema> schemaList){
			this.schemaList = schemaList;
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
		public LazyInitializer(final Class clazz, Schema schema){
			new LazyInitializer(clazz, Collections.singletonList(schema));
		}
		public Object f() {
			initialize(schemaList, test);
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
	
	protected static void initialize(Collection<Schema> schemaList, ClassDaoServiceTest test) {
		test.superClassBeforeMethod();
		new HiveInstaller(test.getConnectString(test.getHiveDatabaseName())).run();		
		ConfigurationReader reader = new ConfigurationReader(test.getEntityClasses());
		reader.install(test.getConnectString(test.getHiveDatabaseName()));
		test.hive = test.getHive();
		for(String nodeName : test.getDataNodeNames())
			try {
				test.hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
				for (Schema schema : schemaList)
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
