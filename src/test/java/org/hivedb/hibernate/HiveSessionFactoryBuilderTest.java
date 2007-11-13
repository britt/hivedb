package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveSessionFactoryBuilderTest extends H2HiveTestCase {
	private EntityHiveConfig config;
	
	@BeforeMethod
	public void configureHive() throws Exception {
		this.config = getEntityHiveConfig();
		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
		new WeatherSchema(getConnectString(getHiveDatabaseName())).install();
	}
	
	@Test
	public void testCreateConfigurationFromNode() throws Exception {
		Node node = new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2);
		Configuration config = HiveSessionFactoryBuilderImpl.createConfigurationFromNode(node);
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
		
		final WeatherReport report = WeatherReportImpl.generate();
		Session session = factoryBuilder.getSessionFactory().openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.openSession(config.getEntityConfig(WeatherReport.class).getPrimaryIndexKey(report));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSessionByResourceId() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		assertNotNull(factoryBuilder.getSessionFactory());
		
		final WeatherReport report = WeatherReportImpl.generate();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		factoryBuilder.openSession("WeatherReport", config.getEntityConfig(WeatherReport.class).getId(report));
	}

	private HiveSessionFactoryBuilderImpl getHiveSessionFactoryBuilder() {
		HiveSessionFactoryBuilderImpl factoryBuilder = 
			new HiveSessionFactoryBuilderImpl(
					getConnectString(getHiveDatabaseName()), 
					Lists.newList(Continent.class, WeatherReport.class),
					new SequentialShardAccessStrategy());
		return factoryBuilder;
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenSessionBySecondaryIndex() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();

		final WeatherReport report = WeatherReportImpl.generate();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		
		factoryBuilder.openSession("WeatherReport", "temperature", report.getTemperature());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInsert() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = WeatherReportImpl.generate();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
	}
	

	@SuppressWarnings("unchecked")
	@Test
	public void testInsertAndRetrieve() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = WeatherReportImpl.generate();
		Session session = factoryBuilder.openSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, session);
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(WeatherReport.class, report.getReportId());
		assertEquals(report, fetched);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDelete() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = WeatherReportImpl.generate();
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
		WeatherReport fetched = (WeatherReport) factoryBuilder.openSession().get(WeatherReport.class, report.getReportId());
		assertEquals(fetched, null);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdate() throws Exception {
		HiveSessionFactoryBuilderImpl factoryBuilder = getHiveSessionFactoryBuilder();
		final WeatherReport report = WeatherReportImpl.generate();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(report);
			}};
		doInTransaction(callback, factoryBuilder.openSession());
		
		final WeatherReport mutated = WeatherReportImpl.generate();
		mutated.setReportId(report.getReportId());
		assertFalse("You have to change something if you want to test update.", mutated.equals(report));
		
		SessionCallback updateCallback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(mutated);
			}};
		
		doInTransaction(updateCallback, factoryBuilder.openSession());
		WeatherReportImpl fetched = (WeatherReportImpl) factoryBuilder.openSession().get(WeatherReport.class, report.getReportId());

		assertFalse(report.equals(fetched));
		assertEquals(mutated, fetched);
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
}
