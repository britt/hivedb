package org.hivedb.hibernate;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Atom;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Date;
import java.util.Properties;

@Config("hive_default")
public class HiveSessionFactoryBuilderTest extends HiveTest {
	
	@Test
	public void testCreateConfigurationFromNode() throws Exception {
		Node node = new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2);
		Configuration config = HiveSessionFactoryBuilderImpl.createConfigurationFromNode(node, new Properties());
		assertEquals(node.getUri(), config.getProperty("hibernate.connection.url"));
		assertEquals(H2Dialect.class.getName(), config.getProperty("hibernate.dialect"));
		assertEquals(node.getId().toString(), config.getProperty("hibernate.connection.shard_id"));
	}
	
	@Test
	public void testGetSessionFactory() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.openSession();
	}
	
	@Test
	public void testOpenSessionByPrimaryKey() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		
		final WeatherReport report = newInstance();
		
		save(factoryBuilder, report);
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.openSession(config.getEntityConfig(getGeneratedClass(WeatherReport.class)).getPartitionKey(report));
	}
	
	@Test
	public void testOpenSessionByResourceId() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		assertNotNull(factoryBuilder.getSessionFactory());
		
		final WeatherReport report = newInstance();
		save(factoryBuilder, report);
		
		factoryBuilder.openSession("WeatherReport", config.getEntityConfig(getGeneratedClass(WeatherReport.class)).getId(report));
	}

	@SuppressWarnings("unchecked")
	private HiveSessionFactoryBuilderImpl getHiveSessionFactoryBuilder() {
		HiveSessionFactoryBuilderImpl factoryBuilder = 
			new HiveSessionFactoryBuilderImpl(
					getConnectString(getHiveDatabaseName()), 
					Lists.newList(Continent.class, WeatherReport.class, WeatherEvent.class),
					new SequentialShardAccessStrategy());
		return factoryBuilder;
	}
	
	@Test
	public void testOpenSessionBySecondaryIndex() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();

		final WeatherReport report = newInstance();
		save(factoryBuilder, report);
		
		factoryBuilder.openSession("WeatherReport", "weatherEventEventId", Atom.getFirstOrThrow(report.getWeatherEvents()).getEventId());
	}
	
	@Test
	public void testInsert() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		for (Node node : getHive().getNodes()) {
			if (! Schemas.tableExists("WEATHER_REPORT", node.getUri())) {
				throw new RuntimeException("Can't find WEATHER_REPORT table on node " + node.getUri());
			}
		}
		save(factoryBuilder, report);
		Hive hive = getHive();
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));		
	}
	
	@Test
	public void testInsertFail() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		
		Session session = factoryBuilder.getSessionFactory().openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		for (Node node : getHive().getNodes()) {
			if (! Schemas.tableExists("WEATHER_REPORT", node.getUri())) {
				throw new RuntimeException("Can't find WEATHER_REPORT table on node " + node.getUri());
			}
		}
		doInTransactionAndFailBeforeCommit(callback, session);
		
		Hive hive = getHive();
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertFalse(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));		
		assertTrue(factoryBuilder.getDefaultInterceptor().isTransient(report));
	}
	
	@Test
	public void testInsertAndRetrieve() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		save(factoryBuilder, report);
		
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		Assert.assertEquals(report, fetched);
	}
	
	@Test
	public void testDelete() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = newInstance();
		save(factoryBuilder, report);
		
		SessionCallback deleteCallback = new SessionCallback(){
			public void execute(Session session) {
				session.delete(report);
			}};
		doInTransaction(deleteCallback, factoryBuilder.openSession());
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(getGeneratedClass(WeatherReport.class), report.getReportId());
		assertEquals(fetched, null);
	}
	
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
		Assert.assertEquals(report, fetched);
	}
	
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
	
	private void save(HiveSessionFactoryBuilderImpl factoryBuilder, final WeatherReport report) {
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
	}
	
	@SuppressWarnings("unchecked")
	private Class<?> getGeneratedClass(Class clazz) {
		return GeneratedClassFactory.getGeneratedClass(clazz);
	}
	private WeatherReport newInstance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
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
