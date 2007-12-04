package org.hivedb.services;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
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
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import sun.util.calendar.BaseCalendar;

public class ClassDaoServiceTest extends H2TestCase {
//	private static int INSTANCE_COUNT = 5;
//	private static ClassDaoService[] services; 
//	private EntityHiveConfig config;
//	static {
////		services = new ClassDaoService[]{ 
////				new BaseClassDaoService(
////						ConfigurationReader.readConfiguration(WeatherReportImpl.class),
////						new BaseDataAccessObject())};
//	}
//	
//	@Override
//	protected void beforeMethod() {
//		super.beforeMethod();
//		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();		
//		ConfigurationReader reader = new ConfigurationReader(getEntityClasses());
//		reader.install(getConnectString(getHiveDatabaseName()));
//		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		for(String nodeName : getDataNodeNames())
//			try {
//				hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
//			} catch (HiveReadOnlyException e) {
//				throw new HiveRuntimeException("Hive was read-only", e);
//			}
//		config = reader.getHiveConfiguration(hive);
//	}
//	
//	protected List<Class<?>> getEntityClasses() {
//		return (List<Class<?>>) Transform.map(new Unary<ClassDaoService, Class<?>>(){
//			public Class<?> f(ClassDaoService item) {
//				return item.getPersistedClass();
//			}}, Arrays.asList(services));
//	}
//
//	@Test(dataProvider = "service")
//	public void saveAndRetrieveInstance(ClassDaoService service) throws Exception {
//		Object original = getPersistentInstance(service);
//		ServiceResponse response = service.get(getId(original));
//		validateRetrieval(original, response);
//	}
//
//	@Test(dataProvider = "service")
//	public void retrieveInstancesUsingAllIndexedProperties(ClassDaoService service) throws Exception {
//		Object original = getPersistentInstance(service);
//		List<Method> indexes = AnnotationHelper.getAllMethodsWithAnnotation(original.getClass(), Index.class);
//		
//		for(Method index : indexes) {
//			String indexPropertyName = BeanUtils.findPropertyForMethod(index).getName();
//			ServiceResponse response = service.getByReference(indexPropertyName, ReflectionTools.invokeGetter(original, indexPropertyName));
//			validateRetrieval(original, response);
//		}
//	}
//	
//	@Test(dataProvider = "service")
//	public void notDetectTheExistenceOfNonPersistentEntities(ClassDaoService service) throws Exception {
//		assertFalse(service.exists(getId(getInstance(service.getPersistedClass()))));
//	}
//	
//	@Test(dataProvider = "service")
//	public void detectTheExistenceOfPersistentEntities(ClassDaoService service) throws Exception {
//		assertFalse(service.exists(getId(getPersistentInstance(service))));
//	}
//	
//	@Test(dataProvider = "service")
//	public void saveMultipleInstances(ClassDaoService service) throws Exception {
//		List<Object> instances = Lists.newArrayList();
//		for(int i=0; i<INSTANCE_COUNT; i++)
//			instances.add(getInstance(service.getPersistedClass()));
//		service.saveAll(instances);
//		for(Object instance : instances)
//			validateRetrieval(instance, service.get(getId(instance)));
//	}
//	
//	@Test(dataProvider = "service")
//	public void deleteAnInstance(ClassDaoService service) throws Exception {
//		Object deleted = getPersistentInstance(service);
//		service.delete(getId(deleted));
//		assertFalse(service.exists(getId(deleted)));
//	}
//	
//	private Object getInstance(Class<Object> clazz) throws Exception {
//		return new BeanGenerator<Object>(clazz).generate();
//	}
//	
//	private Object getPersistentInstance(ClassDaoService service) throws Exception {
//		return service.save(getInstance(service.getPersistedClass()));
//	}
//	
//	private Serializable getId(Object instance) {
//		return config.getEntityConfig(instance.getClass()).getId(instance);
//	}
//	
//	private void validateRetrieval(Object original, ServiceResponse response) {
//		assertEquals("Expected only one instance", 1, response.getInstances());
//		validate(original, Atom.getFirstOrThrow(response.getInstances()));
//	}
//	
//	private void validate(Object expected, Object actual) {
//		assertEquals(expected, actual);
//	}
//	
//	@DataProvider(name = "service")
//	private Object[][] getServices() {
//		Object[][] argz = new Object[services.length][1];
//		int idx = 0;
//		for(ClassDaoService service : services) {
//			argz[idx] = new Object[]{service};
//			idx++;
//		}
//		return argz;
//	}
//	
//	@Override
//	public Collection<String> getDatabaseNames() {
//		Collection<String> dbs = Lists.newArrayList();
//		dbs.addAll(getDataNodeNames());
//		if(!dbs.contains(getHiveDatabaseName()))
//			dbs.add(getHiveDatabaseName());
//		return dbs;
//	}
//
//	private Collection<String> getDataNodeNames() {
//		return Collections.singletonList(getHiveDatabaseName());
//	}
//
//	private String getHiveDatabaseName() {
//		return "hive";
//	}
//

}
