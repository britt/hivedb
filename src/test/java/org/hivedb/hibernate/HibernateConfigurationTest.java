package org.hivedb.hibernate;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.MySQLInnoDBDialect;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class HibernateConfigurationTest {

	@Test
	public void testConfiguration() throws Exception {
		Configuration config = new Configuration();
		config.configure();
		config.setProperty("hibernate.session_factory_name", "factory:bahbah");
		
		config.setProperty("hibernate.dialect", MySQLInnoDBDialect.class.getName());
		config.setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(HiveDbDialect.MySql));
		config.setProperty("hibernate.connection.url", "jdbc:mysql://localhost/blah");
		
		config.setProperty("hibernate.connection.shard_id", new Integer(7).toString());
		config.setProperty("hibernate.shard.enable_cross_shard_relationship_checks", "true");
		
		assertEquals("factory:bahbah", config.getProperty("hibernate.session_factory_name"));
		assertEquals(MySQLInnoDBDialect.class.getName(), config.getProperty("hibernate.dialect"));
		assertEquals(DriverLoader.getDriverClass(HiveDbDialect.MySql), config.getProperty("hibernate.connection.driver_class"));
		assertEquals("jdbc:mysql://localhost/blah", config.getProperty("hibernate.connection.url"));
		assertEquals(new Integer(7).toString(), config.getProperty("hibernate.connection.shard_id"));
		assertEquals("true", config.getProperty("hibernate.shard.enable_cross_shard_relationship_checks"));
		//NOTE: this value is taken from hibernate.cfg.xml
		assertEquals("32", config.getProperty("hibernate.dbcp.maxActive"));
		config.buildSessionFactory();
	}
	
}
