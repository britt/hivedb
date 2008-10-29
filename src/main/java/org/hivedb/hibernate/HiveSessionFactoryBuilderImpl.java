package org.hivedb.hibernate;

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
import org.hibernate.shards.session.ShardedSessionFactory;
import org.hibernate.shards.session.ShardedSessionImpl;
import org.hibernate.shards.strategy.ShardStrategy;
import org.hibernate.shards.strategy.ShardStrategyFactory;
import org.hibernate.shards.strategy.ShardStrategyImpl;
import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.Synchronizeable;
import org.hivedb.configuration.entity.EntityConfig;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.Node;
import org.hivedb.util.Combiner;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Filter.BinaryPredicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Transform.IdentityFunction;
import org.hivedb.util.functional.Unary;

import java.util.*;
import java.util.Map.Entry;

// TODO Node Set session factories have to go, combinatoric compexity

public class HiveSessionFactoryBuilderImpl implements HiveSessionFactoryBuilder, HiveSessionFactory, Observer, Synchronizeable {

  private static final int NODE_SET_LIMIT = 1;
  private static Map<HiveDbDialect, Class<?>> dialectMap = buildDialectMap();
  private Map<Set<Integer>, SessionFactory> nodeSessionFactories;
  private Collection<Class<?>> hibernateClasses;
  private EntityHiveConfig config;
  private ShardAccessStrategy accessStrategy;
  private Properties overrides = new Properties();
  private ShardedSessionFactory allNodesSessionFactory = null;
  private Hive hive;

  public HiveSessionFactoryBuilderImpl(String hiveUri, List<Class<?>> hibernateClasses, ShardAccessStrategy strategy) {
//    hive = Hive.load(hiveUri, CachingDataSourceProvider.getInstance());
//    this.hibernateClasses = hibernateClasses;
//    initialize(buildHiveConfiguration(hibernateClasses), hive, strategy);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public HiveSessionFactoryBuilderImpl(String hiveUri, List<Class<?>> mappedClasses, ShardAccessStrategy strategy, Properties overrides) {
    this(hiveUri, mappedClasses, strategy);
    this.overrides = overrides;
  }

  public HiveSessionFactoryBuilderImpl(EntityHiveConfig config, Hive hive, ShardAccessStrategy strategy) {
    this.hive = hive;
    this.hibernateClasses = flattenWithAssociatedClasses(config);
    initialize(config, hive, strategy);
  }

  public HiveSessionFactoryBuilderImpl(EntityHiveConfig config, Collection<Class<?>> mappedClasses, Hive hive, ShardAccessStrategy strategy) {
    this.hive = hive;
    this.hibernateClasses = mappedClasses;
    initialize(config, hive, strategy);
  }

  @SuppressWarnings("unchecked")
  private Collection<Class<?>> flattenWithAssociatedClasses(EntityHiveConfig config) {
    return Filter.getUnique(Transform.flatten(
      Transform.map(new Unary<EntityConfig, Class<?>>() {
        public Class<?> f(EntityConfig entityConfig) {
          return entityConfig.getRepresentedInterface();
        }
      }, config.getEntityConfigs()),
      Transform.flatMap(new Unary<EntityConfig, Collection<Class<?>>>() {
        public Collection<Class<?>> f(EntityConfig entityConfig) {
          return entityConfig.getAssociatedClasses();
        }
      }, config.getEntityConfigs())));
  }

  private void initialize(EntityHiveConfig config, Hive hive, ShardAccessStrategy strategy) {
    this.accessStrategy = strategy;
    this.config = config;
    this.nodeSessionFactories = buildNodeSetSessionFactories();
    this.allNodesSessionFactory = buildAllNodesSessionFactory();
  }

  public ShardedSessionFactory getSessionFactory() {
    return allNodesSessionFactory;
  }

  private Map<Set<Integer>, SessionFactory> buildNodeSetSessionFactories() {
    final Map<Integer, Configuration> hibernateConfigs = getConfigurationsFromNodes(hive);

    // Build non-sharded session factories for individual single-shard access
    final Map<Integer, SessionFactory> hibernateSessionFactories = Transform.toMap(
      new Transform.IdentityFunction<Integer>(),
      new Unary<Integer, SessionFactory>() {
        public SessionFactory f(Integer nodeId) {
          return hibernateConfigs.get(nodeId).buildSessionFactory();
        }
      },
      hibernateConfigs.keySet());

    Collection<Set<Integer>> nodeSetCombinations = Combiner.generateSets(hibernateConfigs.keySet(), NODE_SET_LIMIT);
    return Transform.toMap(
      new IdentityFunction<Set<Integer>>(),
      new Unary<Set<Integer>, SessionFactory>() {
        public SessionFactory f(Set<Integer> nodeSet) {
          return (nodeSet.size() == 1)
            ? hibernateSessionFactories.get(Atom.getFirstOrThrow(nodeSet)) // non-sharded
            : buildMultiNodeSessionFactory(getSomeNodeConfigurations(nodeSet)); // sharded

        }
      }, nodeSetCombinations);
  }

  private ShardedSessionFactory buildAllNodesSessionFactory() {

    List<ShardConfiguration> shardConfigs = getNodeConfigurations();
    return buildMultiNodeSessionFactory(shardConfigs);
  }

  private ShardedSessionFactory buildMultiNodeSessionFactory(List<ShardConfiguration> shardConfigs) {
    Configuration prototypeConfig = buildPrototypeConfiguration();
    ShardedConfiguration shardedConfig = new ShardedConfiguration(prototypeConfig, shardConfigs, buildShardStrategyFactory());
    return shardedConfig.buildShardedSessionFactory();
  }

  private Map<Integer, Configuration> getConfigurationsFromNodes(Hive hive) {
    Map<Integer, Configuration> configMap = Maps.newHashMap();
    for (Node node : hive.getNodes())
      configMap.put(node.getId(), addClassesToConfig(createConfigurationFromNode(node, overrides)));
    return configMap;
  }

  private List<ShardConfiguration> getNodeConfigurations() {
    final Map<Integer, Configuration> nodeToHibernateConfigMap = getConfigurationsFromNodes(hive);
    List<ShardConfiguration> configs = Lists.newArrayList();
    for (Configuration hibernateConfig : nodeToHibernateConfigMap.values())
      configs.add(new ConfigurationToShardConfigurationAdapter(hibernateConfig));
    return configs;
  }

  private List<ShardConfiguration> getSomeNodeConfigurations(Set<Integer> nodeIds) {
    return new ArrayList<ShardConfiguration>(Filter.grepAgainstList(
      nodeIds,
      getNodeConfigurations(),
      new BinaryPredicate<Integer, ShardConfiguration>() {
        @Override
        public boolean f(Integer nodeId, ShardConfiguration shardConfiguration) {
          return shardConfiguration.getShardId().equals(nodeId);
        }
      }));
  }

  private Configuration buildPrototypeConfiguration() {
    Configuration hibernateConfig = null;
    try {
      hibernateConfig = createConfigurationFromNode(Atom.getFirstOrThrow(hive.getNodes()), overrides);
    }
    catch (Exception e) {
      throw new RuntimeException("The hive has no nodes, so it is impossible to build a prototype configuration");
    }
    addClassesToConfig(hibernateConfig);
    hibernateConfig.setProperty("hibernate.session_factory_name", "factory:prototype");
    return hibernateConfig;
  }

  private Configuration addClassesToConfig(Configuration hibernateConfig) {
    for (Class<?> clazz : hibernateClasses)
      hibernateConfig.addClass(EntityResolver.getPersistedImplementation(clazz));
    return hibernateConfig;
  }

  private EntityHiveConfig buildHiveConfiguration(Collection<Class<?>> classes) {
    return new ConfigurationReader(classes).getHiveConfiguration();
  }

  private ShardStrategyFactory buildShardStrategyFactory() {
    return new ShardStrategyFactory() {
      public ShardStrategy newShardStrategy(List<ShardId> shardIds) {
        return new ShardStrategyImpl(
          new HiveShardSelector(config, hive),
          new HiveShardResolver(config, hive),
          accessStrategy);
      }
    };
  }

  public static Configuration createConfigurationFromNode(Node node, Properties overrides) {
    Configuration config = new Configuration().configure();
    config.setProperty("hibernate.session_factory_name", "factory:" + node.getName());

    config.setProperty("hibernate.dialect", dialectMap.get(node.getDialect()).getName());
    config.setProperty("hibernate.connection.driver_class", DriverLoader.getDriverClass(node.getDialect()));
    config.setProperty("hibernate.connection.url", node.getUri());

    config.setProperty("hibernate.connection.shard_id", node.getId().toString());
    config.setProperty("hibernate.shard.enable_cross_shard_relationship_checks", "true");
    for (Entry<Object, Object> prop : overrides.entrySet())
      config.setProperty(prop.getKey().toString(), prop.getValue().toString());

    return config;
  }

  public void update(Observable o, Object arg) {
    sync();
  }

  public boolean sync() {
    ShardedSessionFactory newFactory = buildAllNodesSessionFactory();
    synchronized (this) {
      this.allNodesSessionFactory = newFactory;
    }
    return true;
  }

  private static Map<HiveDbDialect, Class<?>> buildDialectMap() {
    Map<HiveDbDialect, Class<?>> map = Maps.newHashMap();
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
    return addOpenSessionEvents(allNodesSessionFactory.openSession(interceptor));
  }

  private Session addOpenSessionEvents(Session session) {
    for (Shard shard : ((ShardedSessionImpl) session).getShards()) {
      shard.addOpenSessionEvent(new RecordNodeOpenSessionEvent());
    }
    return session;
  }

  // SessionImpl

  public Session openSession(Object primaryIndexKey) {
    return openSession(
      getNodeIdsOrThrow(primaryIndexKey),
      getDefaultInterceptor());
  }

  private Collection<Integer> getNodeIdsOrThrow(Object primaryIndexKey) {
    final Collection<Integer> nodeIds = hive.directory().getNodeIdsOfPrimaryIndexKey(primaryIndexKey);
    if (nodeIds.size() == 0)
      throw new HiveKeyNotFoundException(String.format("Primary index key %s was not found on any nodes", primaryIndexKey));
    return nodeIds;
  }

  public Session openSession(Object primaryIndexKey, Interceptor interceptor) {
    return openSession(
      getNodeIdsOrThrow(primaryIndexKey),
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

  @SuppressWarnings("unchecked")
  private Session openSession(Collection<Integer> nodeIds, Interceptor interceptor) {
    // We only create SessionFactories for 1 to NODE_SET_LIMIT nodes.
    // If more are requested then we delegate to the allNodesSessionFactory
    if (nodeIds.size() <= NODE_SET_LIMIT) {
      Session session = nodeSessionFactories.get(new HashSet(nodeIds)).openSession(interceptor);
      RecordNodeOpenSessionEvent.setNode(session);
      return session;
    } else {
      return allNodesSessionFactory.openSession(interceptor);
    }
  }

  public Interceptor getDefaultInterceptor() {
    return new HiveInterceptorDecorator(config, hive);
  }

  @SuppressWarnings("unchecked")
  public SessionFactory getSessionFactory(Integer nodeId) {
    return nodeSessionFactories.get(new HashSet(Arrays.asList(nodeId)));
  }
}
