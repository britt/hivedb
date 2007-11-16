package org.hivedb.hibernate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.hibernate.Interceptor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.MySQLInnoDBDialect;
import org.hibernate.shards.ShardId;
import org.hibernate.shards.ShardedConfiguration;
import org.hibernate.shards.cfg.ConfigurationToShardConfigurationAdapter;
import org.hibernate.shards.cfg.ShardConfiguration;
import org.hibernate.shards.engine.ShardedSessionFactoryImplementor;
import org.hibernate.shards.strategy.ShardStrategy;
import org.hibernate.shards.strategy.ShardStrategyFactory;
import org.hibernate.shards.strategy.ShardStrategyImpl;
import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.Synchronizeable;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;

public class HiveSessionFactoryBuilderImpl implements HiveSessionFactoryBuilder, HiveSessionFactory, Observer, Synchronizeable {	
	private static Map<HiveDbDialect, Class<?>> dialectMap = buildDialectMap();
	private Map<Integer, SessionFactory> nodeSessionFactories;
	private EntityHiveConfig config;
	private ShardAccessStrategy accessStrategy;
	
	private ShardedSessionFactoryImplementor factory = null;
	
	public HiveSessionFactoryBuilderImpl(String hiveUri, List<Class<?>> classes, ShardAccessStrategy strategy) {
		initialize(buildHiveConfiguration(Hive.load(hiveUri), classes), strategy);
	}
	
	public HiveSessionFactoryBuilderImpl(EntityHiveConfig config, ShardAccessStrategy strategy) {
		initialize(config, strategy);
	}
	
	private void initialize(EntityHiveConfig config, ShardAccessStrategy strategy) {
		this.accessStrategy = strategy;
		this.config = config;
		this.nodeSessionFactories = Maps.newHashMap();
		this.factory = buildBaseSessionFactory();
		config.getHive().addObserver(this);
	}
	
	public ShardedSessionFactoryImplementor getSessionFactory() {
		return factory;
	}
	
	private ShardedSessionFactoryImplementor buildBaseSessionFactory() {
		Map<Integer, Configuration> hibernateConfigs = getConfigurationsFromNodes(config.getHive());
		
		for(Map.Entry<Integer,Configuration> entry : hibernateConfigs.entrySet()) {
			Configuration cfg = entry.getValue();
			for(EntityConfig entityConfig : config.getEntityConfigs())
				cfg.addClass(entityConfig.getRepresentedInterface());
			
			this.nodeSessionFactories.put(entry.getKey(), cfg.buildSessionFactory());
		}
		
		List<ShardConfiguration> shardConfigs = getNodeConfigurations(hibernateConfigs);
		Configuration prototypeConfig = buildPrototypeConfiguration();
		ShardedConfiguration shardedConfig = new ShardedConfiguration(prototypeConfig, shardConfigs, buildShardStrategyFactory());
		return (ShardedSessionFactoryImplementor) shardedConfig.buildShardedSessionFactory();
	}
	
	private Map<Integer, Configuration> getConfigurationsFromNodes(HiveFacade hive) {
		Map<Integer, Configuration> configMap = Maps.newHashMap();
		for(Node node : hive.getNodes())
			configMap.put(node.getId(), createConfigurationFromNode(node));
		return configMap;
	}
	
	private List<ShardConfiguration> getNodeConfigurations(Map<Integer, Configuration> configMap) {
		List<ShardConfiguration> configs = Lists.newArrayList();
		for(Configuration config : configMap.values())
			configs.add(new ConfigurationToShardConfigurationAdapter(config));
		return configs;
	}

	private Configuration buildPrototypeConfiguration() {
		Configuration hibernateConfig = createConfigurationFromNode(Atom.getFirstOrThrow(config.getHive().getNodes()));
		for(EntityConfig entityConfig : config.getEntityConfigs())
			hibernateConfig.addClass(entityConfig.getRepresentedInterface());
		hibernateConfig.setProperty("hibernate.session_factory_name", "factory:prototype");
		return hibernateConfig;
	}
	
	private EntityHiveConfig buildHiveConfiguration(Hive hive, Collection<Class<?>> classes) {
		ConfigurationReader configReader = new ConfigurationReader(classes);
		return configReader.getHiveConfiguration(hive);
	}
	
	private ShardStrategyFactory buildShardStrategyFactory() {
		return new ShardStrategyFactory() {
			public ShardStrategy newShardStrategy(List<ShardId> shardIds) {
				return new ShardStrategyImpl(
						new HiveShardSelector(config), 
						new HiveShardResolver(config),
						accessStrategy);
			}
		};
	}

	public static Configuration createConfigurationFromNode(Node node) {
		Configuration config = new Configuration().configure();
		config.setProperty("hibernate.session_factory_name", "factory:"+node.getName());
		
		config.setProperty("hibernate.dialect", dialectMap.get(node.getDialect()).getName());
		config.setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(node.getDialect()));
		config.setProperty("hibernate.connection.url", node.getUri());
		
		config.setProperty("hibernate.connection.shard_id", new Integer(node.getId()).toString());
		config.setProperty("hibernate.shard.enable_cross_shard_relationship_checks", "true");
		return config;
	}

	public void update(Observable o, Object arg) {
		sync();
	}

	public boolean sync() {
		ShardedSessionFactoryImplementor newFactory = buildBaseSessionFactory();
		synchronized(this) {
			this.factory = newFactory;
		}
		return true;
	}
	
	private static Map<HiveDbDialect, Class<?>> buildDialectMap() {
		Map<HiveDbDialect,Class<?>> map = Maps.newHashMap();
		map.put(HiveDbDialect.H2, H2Dialect.class);
		map.put(HiveDbDialect.MySql, MySQLInnoDBDialect.class);
		return map;
	}

	public Session openSession() {
		return factory.openSession(getHiveInterceptor());
	}

	public Session openSession(Interceptor interceptor) {
		return factory.openSession(interceptor);
	}

	public Session openSession(Object primaryIndexKey) {
		return openSession(
				config.getHive().directory().getNodeIdsOfPrimaryIndexKey(primaryIndexKey), 
				getHiveInterceptor());
	}

	public Session openSession(Object primaryIndexKey, Interceptor interceptor) {
		return openSession(
				config.getHive().directory().getNodeIdsOfPrimaryIndexKey(primaryIndexKey), 
				interceptor);
	}

	public Session openSession(String resource, Object resourceId) {
		return openSession(
				config.getHive().directory().getNodeIdsOfResourceId(resource, resourceId),
				getHiveInterceptor());
	}

	public Session openSession(String resource, Object resourceId, Interceptor interceptor) {
		return openSession(
				config.getHive().directory().getNodeIdsOfResourceId(resource, resourceId),
				interceptor);
	}

	public Session openSession(String resource, String indexName, Object secondaryIndexKey) {
		return openSession(
				config.getHive().directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey),
				getHiveInterceptor());
	}

	public Session openSession(String resource, String indexName, Object secondaryIndexKey, Interceptor interceptor) {
		return openSession(
				config.getHive().directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey),
				interceptor);
	}
	
	private Session openSession(Collection<Integer> nodeIds, Interceptor interceptor) {
		if(nodeIds.size() == 1)
			return nodeSessionFactories.get(Atom.getFirstOrThrow(nodeIds)).openSession(interceptor);
		else
			throw new UnsupportedOperationException("This operation is not yet implemneted " + nodeIds.size());
	}
	
	private HiveInterceptorDecorator getHiveInterceptor() {
		return new HiveInterceptorDecorator(config);
	}
}
