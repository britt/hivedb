package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import sun.security.x509.RFC822Name;

public class HiveSessionFactoryBuilderTest extends H2HiveTestCase {
	private EntityHiveConfig config;
	
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		this.config = getEntityHiveConfig();
	}
	
	@Test
	public void testCreateConfigurationFromNode() throws Exception {
		Node node = new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2);
		Configuration config = HiveSessionFactoryBuilderImpl.createConfigurationFromNode(node, new Properties());
		assertEquals(node.getUri(), config.getProperty("hibernate.connection.url"));
		assertEquals(H2Dialect.class.getName(), config.getProperty("hibernate.dialect"));
		assertEquals(node.getId().toString(), config.getProperty("hibernate.connection.shard_id"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetSessionFactory() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.openSession();
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSessionByPrimaryKey() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.getSessionFactory().openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.openSession(config.getEntityConfig(getGeneratedClass(WeatherReport.class)).getPrimaryIndexKey(report));
	}

	private WeatherReport newInstance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSessionByResourceId() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		assertNotNull(factoryBuilder.getSessionFactory());
		
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		factoryBuilder.openSession("WeatherReport", config.getEntityConfig(getGeneratedClass(WeatherReport.class)).getId(report));
	}

	private HiveSessionFactoryBuilderImpl getHiveSessionFactoryBuilder() {
		HiveSessionFactoryBuilderImpl factoryBuilder = 
			new HiveSessionFactoryBuilderImpl(
					getConnectString(getHiveDatabaseName()), 
					Lists.newList(Continent.class, WeatherReport.class, WeatherEvent.class),
					new SequentialShardAccessStrategy());
		return factoryBuilder;
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSessionBySecondaryIndex() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();

		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		factoryBuilder.openSession("WeatherReport", "weatherEventEventId", Atom.getFirstOrThrow(report.getWeatherEvents()).getEventId());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInsert() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		for (Node node : getHive().getNodes())
			if (!new WeatherSchema(node.getUri()).tableExists("WEATHER_REPORT"))
				throw new RuntimeException("Can't find WEATHER_REPORT table on node " + node.getUri());
		doInTransaction(callback, session);
		
		Hive hive = getHive();
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInsertFail() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		for (Node node : getHive().getNodes())
			if (!new WeatherSchema(node.getUri()).tableExists("WEATHER_REPORT"))
				throw new RuntimeException("Can't find WEATHER_REPORT table on node " + node.getUri());
		doInTransactionAndFailBeforeCommit(callback, session);
		
		Hive hive = getHive();
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertFalse(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));		
	}
	

	@SuppressWarnings("unchecked")
	@Test
	public void testInsertAndRetrieve() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		Assert.assertEquals(report, fetched, ReflectionTools.getDifferingFields(report, fetched, WeatherReport.class).toString());
	}

	private Class<?> getGeneratedClass(Class clazz) {
		return GeneratedInstanceInterceptor.getGeneratedClass(clazz);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDelete() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		SessionCallback deleteCallback = new SessionCallback(){
			public void execute(Session session) {
				session.delete(report);
			}};
		doInTransaction(deleteCallback, factoryBuilder.openSession());
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		assertEquals(fetched, null);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdate() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, factoryBuilder.openSession());
		
		final WeatherReport mutated = newInstance();
		//System.err.println(((WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId())).getWeatherEvents().size());
		
		GeneratedInstanceInterceptor.setProperty(mutated, "reportId", report.getReportId());
		GeneratedInstanceInterceptor.setProperty(mutated, "continent", report.getContinent());
		GeneratedInstanceInterceptor.setProperty(mutated, "temperature", report.getTemperature());
		GeneratedInstanceInterceptor.setProperty(mutated, "reportTime", new Date(System.currentTimeMillis()));
		// Updating collection items requires more advanced persistence login (see BaseDataAccessObject)
		// so we leave these values identical
		GeneratedInstanceInterceptor.setProperty(mutated, "weatherEvents", report.getWeatherEvents());
		GeneratedInstanceInterceptor.setProperty(mutated, "sources", report.getSources());
		
		assertTrue("You have to change something if you want to test update.", ReflectionTools.getDifferingFields(report, mutated, WeatherReport.class).size() != 0);
		
		SessionCallback updateCallback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(mutated);
			}};
		doInTransaction(updateCallback, factoryBuilder.openSession());
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		assertNotNull(fetched);
		assertFalse(ReflectionTools.getDifferingFields(report, fetched, WeatherReport.class).size() == 0);
		Assert.assertEquals(report, fetched, ReflectionTools.getDifferingFields(mutated, fetched, WeatherReport.class).toString());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdateFail() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, factoryBuilder.openSession());
		
		final WeatherReport mutated = newInstance();
		//System.err.println(((WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId())).getWeatherEvents().size());
		
		GeneratedInstanceInterceptor.setProperty(mutated, "reportId", report.getReportId());
		GeneratedInstanceInterceptor.setProperty(mutated, "continent", report.getContinent());
		GeneratedInstanceInterceptor.setProperty(mutated, "temperature", report.getTemperature());
		GeneratedInstanceInterceptor.setProperty(mutated, "reportTime", new Date(System.currentTimeMillis()));
		// Updating collection items requires more advanced persistence login (see BaseDataAccessObject)
		// so we leave these values identical
		GeneratedInstanceInterceptor.setProperty(mutated, "weatherEvents", report.getWeatherEvents());
		GeneratedInstanceInterceptor.setProperty(mutated, "sources", report.getSources());
		
		assertTrue(
				"You have to change something if you want to test update.", 
				ReflectionTools.getDifferingFields(report, mutated, WeatherReport.class).size() != 0);
		
		SessionCallback updateCallback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(mutated);
			}};
		doInTransactionAndFailBeforeCommit(updateCallback, factoryBuilder.openSession());
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		assertNotNull(fetched);
		assertEquals(ReflectionTools.getDifferingFields(report, fetched, WeatherReport.class).toString(), report, fetched);
		Assert.assertFalse(ReflectionTools.getDifferingFields(mutated, fetched, WeatherReport.class).size() == 0);
	}
	
	public static void doInTransaction(SessionCallback callback, Session session) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			callback.execute(session);
			tx.commit();
		} catch( RuntimeException e ) {
			if(tx != null)
				tx.rollback();
			throw e;
		} finally {
			session.close();
		}
	}
	
	public static void doInTransactionDontCloseSession(SessionCallback callback, Session session) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			callback.execute(session);
			tx.commit();
		} catch( RuntimeException e ) {
			if(tx != null)
				tx.rollback();
			throw e;
		}
	}
	
	public static void doInTransactionAndFailBeforeCommit(SessionCallback callback, Session session) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			callback.execute(session);
			throw new RuntimeException("mreh");
		} catch( RuntimeException e ) {
			if(tx != null)
				tx.rollback();
		} finally {
			session.close();
		}
	}
}
