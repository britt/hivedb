package org.hivedb.hibernate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.hibernate.Session;
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
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.Synchronizeable;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;

public class HiveSessionFactoryBuilderImpl implements HiveSessionFactoryBuilder, HiveSessionFactory, Observer, Synchronizeable {	
	private static Map<HiveDbDialect, Class<?>> dialectMap = buildDialectMap();
	private EntityHiveConfig config;
	private ShardAccessStrategy accessStrategy;
	private List<InterceptorDecorator> interceptors = Lists.newArrayList();
	
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
		this.factory = buildBaseSessionFactory();
		config.getHive().addObserver(this);
	}
	
	public ShardedSessionFactoryImplementor getSessionFactory() {
		return factory;
	}
	
	private ShardedSessionFactoryImplementor buildBaseSessionFactory() {
		List<ShardConfiguration> shardConfigs = getNodeConfigurations(config.getHive());
		Configuration prototypeConfig = buildPrototypeConfiguration();
		ShardedConfiguration shardedConfig = new ShardedConfiguration(prototypeConfig, shardConfigs, buildShardStrategyFactory());
		return (ShardedSessionFactoryImplementor) shardedConfig.buildShardedSessionFactory();
	}
	
	private List<ShardConfiguration> getNodeConfigurations(Hive hive) {
		List<ShardConfiguration> configs = Lists.newArrayList();
		for(Node node : hive.getNodes())
			configs.add(new ConfigurationToShardConfigurationAdapter(createConfigurationFromNode(node)));
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
		Configuration config = new Configuration();
		config.setProperty("hibernate.session_factory_name", "factory:"+node.getName());
		// TODO: Configurable pool size
		config.setProperty("hibernate.connection.pool_size", "5");
		
		config.setProperty("hibernate.dialect", dialectMap.get(node.getDialect()).getName());
		config.setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(node.getDialect()));
		//TODO: This might not work
		config.setProperty("hibernate.connection.url", node.getUri());
		
//		String user = node.getUsername() == null ? node.getUsername() : "";
//		config.setProperty("hibernate.connection.username", user);
//		
//		String pw = node.getPassword() == null ? node.getPassword() : "";
//		config.setProperty("hibernate.connection.password", pw);
		
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

	public void appendInterceptor(InterceptorDecorator interceptor) {
		interceptors.add(interceptor);
	}

	public List<InterceptorDecorator> getInterceptors() {
		return interceptors;
	}

	public void insertInterceptor(InterceptorDecorator interceptor, int index) {
		interceptors.add(index, interceptor);
	}

	public Session openSession() {
		return factory.openSession();
	}

	public Session openSession(InterceptorDecorator interceptor) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(Object primaryIndexKey) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(Object primaryIndexKey,
			InterceptorDecorator interceptor) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(String resource, Object resourceId) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(String resource, Object resourceId,
			InterceptorDecorator interceptor) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(String resource, String indexName,
			Object secondaryIndexKey) {
		// TODO Auto-generated method stub
		return null;
	}

	public Session openSession(String resource, String indexName,
			Object secondaryIndexKey, InterceptorDecorator interceptor) {
		// TODO Auto-generated method stub
		return null;
	}

	public void prependInterceptor(InterceptorDecorator interceptor) {
		interceptors.add(0, interceptor);
	}
}
