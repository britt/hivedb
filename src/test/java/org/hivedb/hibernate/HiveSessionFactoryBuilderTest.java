package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertNotNull;

import java.sql.Types;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class HiveSessionFactoryBuilderTest extends H2HiveTestCase {
	
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
		Hive hive = Hive.create(getConnectString(getHiveDatabaseName()), WeatherReport.CONTINENT, Types.VARCHAR);
		ConfigurationReader config = new ConfigurationReader(Continent.class, WeatherReport.class);
		config.install(hive);
		hive.addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
	
		HiveSessionFactoryBuilder factoryBuilder = 
			new HiveSessionFactoryBuilderImpl(
					getConnectString(getHiveDatabaseName()), 
					Lists.newList(Continent.class, WeatherReport.class),
					new SequentialShardAccessStrategy());
		assertNotNull(factoryBuilder.getSessionFactory());
		factoryBuilder.getSessionFactory().openSession();
	}
}
