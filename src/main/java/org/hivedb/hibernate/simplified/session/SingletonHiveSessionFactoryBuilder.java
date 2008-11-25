package org.hivedb.hibernate.simplified.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.shards.ShardId;
import org.hibernate.shards.ShardedConfiguration;
import org.hibernate.shards.cfg.ShardConfiguration;
import org.hibernate.shards.session.ShardedSessionFactory;
import org.hibernate.shards.strategy.ShardStrategy;
import org.hibernate.shards.strategy.ShardStrategyFactory;
import org.hibernate.shards.strategy.ShardStrategyImpl;
import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.Node;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.HiveShardResolver;
import org.hivedb.hibernate.HiveShardSelector;
import org.hivedb.hibernate.simplified.session.configuration.NodeConfiguration;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

import java.util.*;

public class SingletonHiveSessionFactoryBuilder implements HiveSessionFactoryBuilder, Observer {
  private final static Log log = LogFactory.getLog(SingletonHiveSessionFactoryBuilder.class);
  private HiveSessionFactory factory = null;
  private Hive hive;
  private List<Class<?>> persistableClasses;
  private Properties overrides;
  private ShardAccessStrategy accessStrategy;
  private EntityHiveConfig hiveConfig;
  
  public SingletonHiveSessionFactoryBuilder(Hive hive, List<Class<?>> persistableClasses, ShardAccessStrategy accessStrategy) {
    this(hive, persistableClasses, accessStrategy, new Properties());
  }

  public SingletonHiveSessionFactoryBuilder(Hive hive, List<Class<?>> persistableClasses, ShardAccessStrategy accessStrategy, Properties overrides) {
    this.hive = hive;
    this.persistableClasses = persistableClasses;
    this.overrides = overrides;
    this.accessStrategy = accessStrategy;
    this.hiveConfig = new ConfigurationReader(this.persistableClasses).getHiveConfiguration();
  }

  public HiveSessionFactory getSessionFactory() {
    if(factory == null)
      factory = buildSessionFactory();
    return factory;
  }

  private HiveSessionFactory buildSessionFactory() {
    log.info("Building HiveSessionFactory");
    Collection<NodeConfiguration> nodeConfigs = getNodeConfigurations();
    Collection<ShardConfiguration> shardConfigs = Transform.map(new Unary<NodeConfiguration, ShardConfiguration>(){
      public ShardConfiguration f(NodeConfiguration item) {
        log.info("Adding node: " + item.getNode().getUri());
        return item.toShardConfig();
      }
    },nodeConfigs);
    ShardedSessionFactory shardedFactory =
      new ShardedConfiguration(
        buildPrototypeConfiguration(Atom.getFirstOrThrow(nodeConfigs)),
        Lists.newList(shardConfigs),
        buildShardStrategyFactory()).buildShardedSessionFactory();

    logIterable("Adding class:", persistableClasses);
    logMap("Overriding: ", overrides);
    
    return new HiveSessionFactoryImpl(shardedFactory, hive, hiveConfig);
  }

  private ShardStrategyFactory buildShardStrategyFactory() {
		return new ShardStrategyFactory() {
			public ShardStrategy newShardStrategy(List<ShardId> shardIds) {
				return new ShardStrategyImpl(
						new HiveShardSelector(hiveConfig,hive.directory()),
						new HiveShardResolver(hiveConfig,hive),
						accessStrategy);
			}
		};
	}

  private Configuration buildPrototypeConfiguration(Configuration config) {
    for(Class clazz : persistableClasses)
      config.addClass(clazz);
		config.setProperty("hibernate.session_factory_name", "factory:prototype");
		return config;
	}

  private Collection<NodeConfiguration> getNodeConfigurations() {
    return Transform.map(new Unary<Node,NodeConfiguration>(){
      public NodeConfiguration f(Node item) {
        return (NodeConfiguration) new NodeConfiguration(item, overrides).configure();
      }
    }, hive.getNodes());
  }

  public void update(Observable o, Object arg) {
    log.info("Update received");
    HiveSessionFactory newFactory = buildSessionFactory();
		synchronized(this) {
			this.factory = newFactory;
		}
  }

  private void logIterable(String prefix, Iterable<?> iterable) {
    for(Object o : iterable) {
      log.info(String.format("%s %s", prefix, o));
    }
  }

  private void logMap(String prefix, Map<?,?> map) {
    for(Map.Entry e : map.entrySet()) {
      log.info(String.format("%s %s => %s", prefix, e.getKey(), e.getValue()));
    }
  }

  public Properties getOverrides() {
    return overrides;
  }

  public List<Class<?>> getPersistableClasses() {
    return persistableClasses;
  }
}

