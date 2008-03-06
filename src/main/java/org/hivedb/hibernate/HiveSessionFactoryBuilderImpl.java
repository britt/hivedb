package org.hivedb.hibernate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.Map.Entry;

import org.hibernate.Interceptor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.MySQLInnoDBDialect;
import org.hibernate.shards.Shard;
import org.hibernate.shards.ShardId;
import org.hibernate.shards.ShardedConfiguration;
import org.hibernate.shards.cfg.ConfigurationToShardConfigurationAdapter;
import org.hibernate.shards.cfg.ShardConfiguration;
import org.hibernate.shards.engine.ShardedSessionFactoryImplementor;
import org.hibernate.shards.session.ShardedSessionImpl;
import org.hibernate.shards.strategy.ShardStrategy;
import org.hibernate.shards.strategy.ShardStrategyFactory;
import org.hibernate.shards.strategy.ShardStrategyImpl;
import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.Synchronizeable;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;

public class HiveSessionFactoryBuilderImpl implements HiveSessionFactoryBuilder, HiveSessionFactory, Observer, Synchronizeable {	
	private static Map<HiveDbDialect, Class<?>> dialectMap = buildDialectMap();
	private Map<Integer, SessionFactory> nodeSessionFactories;
	private Collection<Class<?>> mappedClasses;
	private EntityHiveConfig config;
	private ShardAccessStrategy accessStrategy;
	private Properties overrides = new Properties();
	private ShardedSessionFactoryImplementor factory = null;
	private Hive hive;
	
	public HiveSessionFactoryBuilderImpl(String hiveUri, List<Class<?>> mappedClasses, ShardAccessStrategy strategy) {
		hive = Hive.load(hiveUri);
		this.mappedClasses = mappedClasses;
		initialize(buildHiveConfiguration(hive, mappedClasses), hive, strategy);
	}
	
	public HiveSessionFactoryBuilderImpl(String hiveUri, List<Class<?>> mappedClasses, ShardAccessStrategy strategy, Properties overrides) {
		this(hiveUri, mappedClasses, strategy);
		this.overrides = overrides;
	}
	
	public HiveSessionFactoryBuilderImpl(EntityHiveConfig config, Hive hive, ShardAccessStrategy strategy) {
		this.hive = hive;
		this.mappedClasses = new EntityResolver(config).getEntityClasses();
		initialize(config, hive, strategy);
	}
	
	public HiveSessionFactoryBuilderImpl(EntityHiveConfig config, Collection<Class<?>> mappedClasses, Hive hive, ShardAccessStrategy strategy) {
		this.hive = hive;
		this.mappedClasses = mappedClasses;
		initialize(config, hive, strategy);
	}
	
	private void initialize(EntityHiveConfig config,Hive hive, ShardAccessStrategy strategy) {
		this.accessStrategy = strategy;
		this.config = config;
		this.nodeSessionFactories = Maps.newHashMap();
		this.factory = buildBaseSessionFactory();
		hive.addObserver(this);
	}
	
	public ShardedSessionFactoryImplementor getSessionFactory() {
		return factory;
	}
	
	private ShardedSessionFactoryImplementor buildBaseSessionFactory() {
		Map<Integer, Configuration> hibernateConfigs = getConfigurationsFromNodes(hive);
		
		for(Map.Entry<Integer,Configuration> entry : hibernateConfigs.entrySet()) {
			Configuration cfg = entry.getValue();
			addConfigurations(cfg);
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
			configMap.put(node.getId(), createConfigurationFromNode(node, overrides));
		return configMap;
	}
	
	private List<ShardConfiguration> getNodeConfigurations(Map<Integer, Configuration> configMap) {
		List<ShardConfiguration> configs = Lists.newArrayList();
		for(Configuration config : configMap.values())
			configs.add(new ConfigurationToShardConfigurationAdapter(config));
		return configs;
	}

	private Configuration buildPrototypeConfiguration() {
		Configuration hibernateConfig = null;
		try {
			hibernateConfig = createConfigurationFromNode(Atom.getFirstOrThrow(hive.getNodes()), overrides);
		}
		catch (Exception e) {
			throw new RuntimeException("The hive has no nodes, so it is impossible to build a prototype configuration");
		}
		addConfigurations(hibernateConfig);
		hibernateConfig.setProperty("hibernate.session_factory_name", "factory:prototype");
		return hibernateConfig;
	}

	private void addConfigurations(Configuration hibernateConfig) {
		for(Class<?> clazz : mappedClasses) 
			hibernateConfig.addClass(EntityResolver.getPersistedImplementation(clazz));
	}
	
	private EntityHiveConfig buildHiveConfiguration(Hive hive, Collection<Class<?>> classes) {
		return new ConfigurationReader(classes).getHiveConfiguration();
	}
	
	private ShardStrategyFactory buildShardStrategyFactory() {
		return new ShardStrategyFactory() {
			public ShardStrategy newShardStrategy(List<ShardId> shardIds) {
				return new ShardStrategyImpl(
						new HiveShardSelector(config,hive), 
						new HiveShardResolver(config,hive),
						accessStrategy);
			}
		};
	}

	public static Configuration createConfigurationFromNode(Node node, Properties overrides) {
		Configuration config = new Configuration().configure();
		config.setProperty("hibernate.session_factory_name", "factory:"+node.getName());
		
		config.setProperty("hibernate.dialect", dialectMap.get(node.getDialect()).getName());
		config.setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(node.getDialect()));
		config.setProperty("hibernate.connection.url", node.getUri());
		
		config.setProperty("hibernate.connection.shard_id", new Integer(node.getId()).toString());
		config.setProperty("hibernate.shard.enable_cross_shard_relationship_checks", "true");
		for(Entry<Object,Object> prop : overrides.entrySet()) 
			config.setProperty(prop.getKey().toString(), prop.getValue().toString());
		
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
		
	// ShardedSessionImpl
	
	public Session openAllShardsSession() {
		return openAllShardsSession(getDefaultInterceptor());
	}
	
	public Session openSession() {
		return openAllShardsSession();
	}

	public Session openSession(Interceptor interceptor) {
		return openAllShardsSession(interceptor);
	}
	
	private Session openAllShardsSession(Interceptor interceptor) {
		return addOpenSessionEvents(factory.openSession(interceptor));
	}
	
	private Session addOpenSessionEvents(Session session) {
		for (Shard shard : ((ShardedSessionImpl) session).getShards()) {
			shard.addOpenSessionEvent(new OpenSessionEventImpl());
		}
		return session;
	}
	
	// SessionImpl
	
	public Session openSession(Object primaryIndexKey) {
		return openSession(
				hive.directory().getNodeIdsOfPrimaryIndexKey(primaryIndexKey), 
				getDefaultInterceptor());
	}
	
	public Session openSession(Object primaryIndexKey, Interceptor interceptor) {
		return openSession(
				hive.directory().getNodeIdsOfPrimaryIndexKey(primaryIndexKey), 
				interceptor);
	}

	public Session openSession(String resource, Object resourceId) {
		final Collection<Integer> nodeIdsOfResourceId = hive.directory().getNodeIdsOfResourceId(resource, resourceId);
		if (nodeIdsOfResourceId.size() == 0)
			throw new UnsupportedOperationException(String.format("No nodes found for resource id %s of resource %s", resourceId, resource));
		return openSession(
				nodeIdsOfResourceId,
				getDefaultInterceptor());
	}

	public Session openSession(String resource, Object resourceId, Interceptor interceptor) {
		return openSession(
				hive.directory().getNodeIdsOfResourceId(resource, resourceId),
				interceptor);
	}

	public Session openSession(String resource, String indexName, Object secondaryIndexKey) {
		return openSession(
				hive.directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey),
				getDefaultInterceptor());
	}

	public Session openSession(String resource, String indexName, Object secondaryIndexKey, Interceptor interceptor) {
		return openSession(
				hive.directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey),
				interceptor);
	}
	
	private Session openSession(Collection<Integer> nodeIds, Interceptor interceptor) {
		if(nodeIds.size() == 1) {
			Session session = nodeSessionFactories.get(Atom.getFirstOrThrow(nodeIds)).openSession(interceptor);
			OpenSessionEventImpl.setNode(session);
			return session;
		} else {
			throw new UnsupportedOperationException("This operation is not yet implemented " + nodeIds.size());
		}
	}
	
	public Interceptor getDefaultInterceptor() {
		return new HiveInterceptorDecorator(config, hive);
	}
}
