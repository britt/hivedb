package org.hivedb.hibernate.simplified.session.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.shards.cfg.ConfigurationToShardConfigurationAdapter;
import org.hibernate.shards.cfg.ShardConfiguration;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.DriverLoader;

import java.util.Map;
import java.util.Properties;

public class NodeConfiguration extends Configuration {
  private final static Log log = LogFactory.getLog(NodeConfiguration.class);
  private Node node;
  private Properties overrides = new Properties();

  public NodeConfiguration(Node node){
    super();
    this.node = node;
  }

  public NodeConfiguration(Node node, Properties overrides) {
    this.node = node;
    this.overrides = overrides;
  }

  @Override
  public Configuration configure() {
    super.configure();
    setProperty("hibernate.session_factory_name", "factory:"+node.getName());
		setProperty("hibernate.dialect", DialectTools.getHibernateDialect(node.getDialect()).getName());
		setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(node.getDialect()));
		setProperty("hibernate.connection.url", node.getUri());
		setProperty("hibernate.connection.shard_id", node.getId().toString());
		setProperty("hibernate.shard.enable_cross_shard_relationship_checks", "true");
		for(Map.Entry<Object,Object> prop : overrides.entrySet())
			setProperty(prop.getKey().toString(), prop.getValue().toString());
    return this;
  }

  public ShardConfiguration toShardConfig() {
    return new ConfigurationToShardConfigurationAdapter(configure());
  }
}

