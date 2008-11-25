package org.hivedb.hibernate.simplified.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Interceptor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.shards.Shard;
import org.hibernate.shards.session.ShardedSessionFactory;
import org.hibernate.shards.session.ShardedSessionImpl;
import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Node;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.hibernate.RecordNodeOpenSessionEvent;
import org.hivedb.hibernate.simplified.HiveInterceptorDecorator;
import org.hivedb.util.functional.*;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

// TODO Enforce ReadWrite Constraints
public class HiveSessionFactoryImpl implements HiveSessionFactory {
  private final static Log log = LogFactory.getLog(HiveSessionFactoryImpl.class);
  private ShardedSessionFactory factory;
  private EntityHiveConfig config;
  private Hive hive;
  private Map<Integer, SessionFactory> factories;
  public Map<Integer, Node> nodesById;

  public HiveSessionFactoryImpl(ShardedSessionFactory shardedFactory, Hive hive, EntityHiveConfig hiveConfig) {
    this.factory = shardedFactory;
    this.hive = hive;
    this.config = hiveConfig;
    this.factories = buildSessionFactoryMap(factory, hive.getNodes());
    this.nodesById = buildNodeToIdMap(hive);
  }

  public Session openSession() {
    return openShardedSession(getDefaultInterceptor());
  }

  public Session openSession(Interceptor interceptor) {
    return openShardedSession(wrapWithHiveInterceptor(interceptor));
  }

  public Session openSession(Object primaryIndexKey) {
    return openSession(hive.directory().getNodeIdsOfPartitionKey(primaryIndexKey), getDefaultInterceptor());
  }

  public Session openSession(Object primaryIndexKey, Interceptor interceptor) {
    return openSession(hive.directory().getNodeIdsOfPartitionKey(primaryIndexKey), wrapWithHiveInterceptor(interceptor));
  }

  public Session openSession(String resource, Object resourceId) {
    return openSession(hive.directory().getNodeIdsOfResourceId(resource,  resourceId), getDefaultInterceptor());
  }

  public Session openSession(String resource, Object resourceId, Interceptor interceptor) {
    return openSession(hive.directory().getNodeIdsOfResourceId(resource,  resourceId), wrapWithHiveInterceptor(interceptor));
  }

  public Session openSession(String resource, String indexName, Object secondaryIndexKey) {
    return openSession(hive.directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey), getDefaultInterceptor());
  }

  public Session openSession(String resource, String indexName, Object secondaryIndexKey, Interceptor interceptor) {
    return openSession(hive.directory().getNodeIdsOfSecondaryIndexKey(resource, indexName, secondaryIndexKey), wrapWithHiveInterceptor(interceptor));
  }

  public Interceptor getDefaultInterceptor() {
		return new HiveInterceptorDecorator(config, hive);
	}

  public Interceptor wrapWithHiveInterceptor(Interceptor interceptor) {
		return new HiveInterceptorDecorator(interceptor, config, hive);
	}

  private Session openShardedSession(Interceptor interceptor) {
    return addEventsToShardedSession((ShardedSessionImpl) factory.openSession(wrapWithHiveInterceptor(interceptor)));
  }

  private Session openSession(Collection<Integer> nodeIds, Interceptor interceptor) {
    Collection<Node> nodes = getNodesFromIds(nodeIds);
    if(nodes.size() > 1)
      throw new IllegalStateException("Record appears to be stored on more than one node.  Currently HiveDB Hibernate support only allows records to be stored on a single node.");
    return openSession(Atom.getFirstOrThrow(nodes), interceptor);
  }

  private Session openSession(Node node, Interceptor interceptor) {
    return addEventsToSession(getSessionFactory(node).openSession(interceptor));
  }

  private SessionFactory getSessionFactory(Node node) {
    return factories.get(node.getId());
  }

  private Session addEventsToShardedSession(ShardedSessionImpl session) {
		for (Shard shard : session.getShards()) {
			shard.addOpenSessionEvent(new RecordNodeOpenSessionEvent());
		}
		return session;
	}

  private Session addEventsToSession(Session session) {
		RecordNodeOpenSessionEvent.setNode(session);
		return session;
	}

  @SuppressWarnings("deprecation")
  public String extractFactoryURL(SessionFactory factory) {
    Session session = null;
    try {
      session = factory.openSession();
      return session.connection().getMetaData().getURL();
    } catch (SQLException e) {
      throw new HiveRuntimeException(e);
    } finally {
      if(session != null)
        session.close();
    }
  }

  private Collection<Node> getNodesFromIds(Collection<Integer> ids) {
    return Transform.map(new Unary<Integer, Node>(){
      public Node f(Integer item) {
        return nodesById.get(item);
      }
    }, ids);
  }

   private Map<Integer, Node> buildNodeToIdMap(Hive hive) {
    Map<Integer, Node> nodeMap = new HashMap<Integer, Node>();
    for(Node node : hive.getNodes()) {
      nodeMap.put(node.getId(), node);
    }
    return nodeMap;
  }

  private Map<Integer, SessionFactory> buildSessionFactoryMap(ShardedSessionFactory factory, Collection<Node> nodes) {
    Map<Integer, SessionFactory> factoryMap = new HashMap<Integer, SessionFactory>();
    for(SessionFactory f : factory.getSessionFactories()) {
      Node node = matchNodeToFactoryUrl(extractFactoryURL(f), nodes);
      factoryMap.put(node.getId(), f);
    }
    return factoryMap;
  }

  private Node matchNodeToFactoryUrl(final String url, Collection<Node> nodes) {
    return Filter.grepSingle(new Predicate<Node>(){
      public boolean f(Node item) {
        return item.getUri().startsWith(url);
      }
    }, nodes);
  }
}

