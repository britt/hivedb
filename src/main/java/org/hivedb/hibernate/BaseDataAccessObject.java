package org.hivedb.hibernate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.*;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.DataIndexDelegate;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigDelegator;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.util.Lists;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedClassFactory;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.*;
import org.hivedb.util.functional.Filter;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;

public class BaseDataAccessObject implements DataAccessObject<Object, Serializable> {
  private final Log log = LogFactory.getLog(BaseDataAccessObject.class);
  private static int CHUNK_SIZE = 10;
  private final HiveSessionFactory factory;
  private final EntityConfig config;
  private final Class<?> clazz;
  private Hive hive;
  private EntityIndexConfig partitionIndexEntityIndexConfig;

  public Hive getHive() {
    return hive;
  }

  public void setHive(Hive hive) {
    this.hive = hive;
  }

  public BaseDataAccessObject(EntityConfig config, Hive hive, HiveSessionFactory factory) {
    this.clazz = config.getRepresentedInterface();
    this.config = config;
    this.factory = factory;
    this.hive = hive;

  }


  public Boolean exists(Serializable id) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), id);
  }

  public Object get(final Serializable id) {
    try {
      QueryCallback query = new QueryCallback() {
        public Collection<Object> execute(Session session) {
          Object fetched = get(id, session);
          if (fetched == null && exists(id)) {
            try {
              hive.directory().deleteResourceId(config.getResourceName(), id);
            } catch (HiveLockableException e) {
              log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Unable to cleanup record because Hive was read-only.", config.getResourceName(), id));
            }
            log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Directory record removed.", config.getResourceName(), id));
          }
          return Collections.singletonList(fetched);
        }
      };
      Object fetched = Atom.getFirstOrThrow(queryInTransaction(query, getSession()));
      if (fetched == null && exists(id)) {
        try {
          hive.directory().deleteResourceId(config.getResourceName(), id);
        } catch (HiveLockableException e) {
          log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Unable to cleanup record because Hive was read-only.", config.getResourceName(), id));
        }
        log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Directory record removed.", config.getResourceName(), id));
      }
      return fetched;
    } catch (NoSuchElementException e) {
      //TODO previous code may make this logic irrelevant
      //This save us a directory hit for all cases except when requesting a non-existent id.
      if (!exists(id))
        return null;
      else
        throw e;
    }
  }

  public Collection<Object> getPropertyValue(final String propertyName, final int firstResult, final int maxResults) {
    QueryCallback callback = new QueryCallback() {
      @SuppressWarnings("unchecked")
      public Collection<Object> execute(Session session) {
        Query query =
            session.createQuery(
                String.format(
                    "select %s from %s",
                    propertyName,
                    GeneratedClassFactory.getGeneratedClass(config.getRepresentedInterface()).getSimpleName()));
        if (maxResults > 0) {
          query.setFirstResult(firstResult);
          query.setMaxResults(maxResults);
        }
        return query.list();
      }
    };
    return queryInTransaction(callback, getSession());
  }

  public Collection<Object> findByProperty(final String propertyName, final Object propertyValue) {
    return findByProperties(propertyName, Collections.singletonMap(propertyName, propertyValue));
  }

  public Collection<Object> findByProperty(final String propertyName, final Object propertyValue, final Integer firstResult, final Integer maxResults) {
    return findByProperties(propertyName, Collections.singletonMap(propertyName, propertyValue), firstResult, maxResults);
  }

  public Collection<Object> findByProperties(String partitioningPropertyName, final Map<String, Object> propertyNameValueMap) {
    return findByProperties(partitioningPropertyName, propertyNameValueMap, 0, 0);
  }

  public Collection<Object> findByProperties(String partitioningPropertyName, final Map<String, Object> propertyNameValueMap, final Integer firstResult, final Integer maxResults) {
    return queryByProperties(partitioningPropertyName, propertyNameValueMap, firstResult, maxResults, false);
  }

  public Integer getCount(final String propertyName, final Object propertyValue) {
    return (Integer) sumCounts(queryByProperties(propertyName, Collections.singletonMap(propertyName, propertyValue), 0, 0, true));
  }

  public Integer getCountByProperties(String partitioningPropertyName, Map<String, Object> propertyNameValueMap) {
    return (Integer) sumCounts(queryByProperties(partitioningPropertyName, propertyNameValueMap, 0, 0, true));
  }

  public Integer getCountByProperties(String partitioningPropertyName, Map<String, Object> propertyNameValueMap, Integer firstResult, Integer maxResults) {
    return (Integer) sumCounts(queryByProperties(partitioningPropertyName, propertyNameValueMap, firstResult, maxResults, true));
  }

  private Integer sumCounts(Collection<Object> objects) {
    int count = 0;
    for (Object object : objects) {
      count += ((Number) object).intValue();
    }
    return count;
  }

  public Collection<Object> findByPropertyRange(final String propertyName, final Object minValue, final Object maxValue) {
    // Use an AllShardsresolutionStrategy + Criteria
    final EntityConfig entityConfig = config;
    final EntityIndexConfig indexConfig = config.getEntityIndexConfig(propertyName);
    Session session = factory.openAllShardsSession();
    QueryCallback callback;
    if (isPrimitiveCollection(propertyName)) {
      callback = new QueryCallback() {
        @SuppressWarnings("unchecked")
        public Collection<Object> execute(Session session) {
          Query query = session.createQuery(String.format("from %s as x where x.%s between (:minValue, :maxValue)",
              entityConfig.getRepresentedInterface().getSimpleName(),
              indexConfig.getIndexName())
          ).setEntity("minValue", minValue).setEntity("maxValue", maxValue);
          return query.list();
        }
      };
    } else {
      callback = new QueryCallback() {
        @SuppressWarnings("unchecked")
        public Collection<Object> execute(Session session) {
          Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
          addPropertyRangeRestriction(criteria, propertyName, minValue, maxValue);
          return criteria.list();
        }
      };
    }
    return queryInTransaction(callback, session);
  }

  public Integer getCountByRange(final String propertyName, final Object minValue, final Object maxValue) {
    // Use an AllShardsresolutionStrategy + Criteria
    Session session = factory.openAllShardsSession();
    QueryCallback query = new QueryCallback() {
      @SuppressWarnings("unchecked")
      public Collection<Object> execute(Session session) {
        Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        addPropertyRangeRestriction(criteria, propertyName, minValue, maxValue);
        criteria.setProjection(Projections.rowCount());
        return criteria.list();
      }
    };
    return (Integer) Atom.getFirstOrThrow(queryInTransaction(query, session));
  }

  public Collection<Object> findByPropertyRange(final String propertyName, final Object minValue, final Object maxValue, final Integer firstResult, final Integer maxResults) {
    // Use an AllShardsresolutionStrategy + Criteria
    final EntityConfig entityConfig = config;
    final EntityIndexConfig indexConfig = config.getEntityIndexConfig(propertyName);
    Session session = factory.openAllShardsSession();
    QueryCallback callback;
    if (isPrimitiveCollection(propertyName)) {
      callback = new QueryCallback() {
        @SuppressWarnings("unchecked")
        public Collection<Object> execute(Session session) {
          Query query = session.createQuery(String.format("from %s as x where %s between (:minValue, :maxValue) order by x.%s asc limit %s, %s",
              entityConfig.getRepresentedInterface().getSimpleName(),
              indexConfig.getIndexName(),
              entityConfig.getIdPropertyName(),
              firstResult,
              maxResults)
          ).setEntity("minValue", minValue).setEntity("maxValue", maxValue);
          return query.list();
        }
      };
    } else {
      callback = new QueryCallback() {
        @SuppressWarnings("unchecked")
        public Collection<Object> execute(Session session) {
          Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
          addPropertyRangeRestriction(criteria, propertyName, minValue, maxValue);
          criteria.add(Restrictions.between(propertyName, minValue, maxValue));
          criteria.setFirstResult(firstResult);
          criteria.setMaxResults(maxResults);
          criteria.addOrder(Order.asc(propertyName));
          return criteria.list();
        }
      };
    }
    return queryInTransaction(callback, session);
  }

  public Object save(Object entity) {
    final Object populatedEntity = Atom.getFirstOrThrow(populateDataIndexDelegates(Collections.singletonList(entity)));
    SessionCallback callback = new SessionCallback() {
      public void execute(Session session) {
        session.saveOrUpdate(getRespresentedClass().getName(), populatedEntity);
      }
    };

    SessionCallback cleanupCallback = new SessionCallback() {
      public void execute(Session session) {
        session.refresh(populatedEntity);
        session.lock(getRespresentedClass().getName(), populatedEntity, LockMode.UPGRADE);
        session.update(getRespresentedClass().getName(), populatedEntity);
        log.warn(String.format("%s with id %s exists in the data node but not on the directory. Data node record was updated and re-indexed.", config.getResourceName(), config.getId(populatedEntity)));
      }
    };

    if (partionDimensionKeyHasChanged(entity))
      delete(config.getId(entity));
    doSave(populatedEntity, callback, cleanupCallback);
    return entity;
  }

  private boolean partionDimensionKeyHasChanged(Object entity) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), config.getId(entity)) &&
        !config.getPrimaryIndexKey(entity).equals(getHive().directory().getPrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity)));
  }

  public Collection<Object> saveAll(Collection<Object> collection) {
    List<Object> entities = Lists.newList(populateDataIndexDelegates(collection));
    validateNonNull(entities);
    final boolean partitionDimensionKeyHasChanged = partionDimensionKeyHasChanged(Atom.getFirstOrThrow(entities));
    //o large save operations in manageable sized chunks
    for (int chunkId = 0; chunkId < entities.size(); chunkId += BaseDataAccessObject.getSaveChunkSize()) {
      final Collection<Object> chunk =
          entities.subList(
              chunkId,
              chunkId + getSaveChunkSize() <= entities.size() ? chunkId + getSaveChunkSize() : entities.size());

      // If the partition dimension key has changed for any entity we assume that we need
      // to delete all entities before saving, in case the new partition dimension key
      // is on a different node
      if (partitionDimensionKeyHasChanged) {
        SessionCallback callback = new SessionCallback() {
          public void execute(Session session) {
            for (Object entity : chunk) {
              Object deleted = get(config.getId(entity), session);
              session.delete(deleted);
            }
          }
        };
        deleteAll(callback);
      }

      SessionCallback callback = new SessionCallback() {
        public void execute(Session session) {
          for (Object entity : chunk) {
            session.saveOrUpdate(getRespresentedClass().getName(), entity);
          }
        }
      };
      SessionCallback cleanupCallback = new SessionCallback() {
        public void execute(Session session) {
          for (Object entity : chunk) {
            try {
              session.refresh(entity);
            } catch (RuntimeException e) {
              //Damned Hibernate
            }
            if (!exists(config.getId(entity))) {
              if (existsInSession(session, config.getId(entity))) {
                session.lock(getRespresentedClass().getName(), entity, LockMode.UPGRADE);
                session.update(getRespresentedClass().getName(), entity);
                log.warn(String.format("%s with id %s exists in the data node but not on the directory. Data node record was updated and re-indexed.", config.getResourceName(), config.getId(entity)));
              } else {
                session.saveOrUpdate(getRespresentedClass().getName(), entity);
              }
            } else {
              if (!existsInSession(session, config.getId(entity))) {
                try {
                  getHive().directory().deleteResourceId(config.getResourceName(), config.getId(entity));
                } catch (HiveLockableException e) {
                  log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Unable to cleanup record because Hive was read-only.", config.getResourceName(), config.getId(entity)));
                }
                log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Directory record removed.", config.getResourceName(), config.getId(entity)));
              }
              session.saveOrUpdate(getRespresentedClass().getName(), entity);
            }
          }
        }
      };
      doSaveAll(chunk, callback, cleanupCallback);
    }

    return collection;
  }

  public Serializable delete(final Serializable id) {
    SessionCallback callback = new SessionCallback() {
      public void execute(Session session) {
        Object deleted = get(id, session);
        session.delete(deleted);
      }
    };
    doInTransaction(callback, getSession());
    return id;
  }

  private Object get(Serializable id, Session session) {
    return session.get(getRespresentedClass(), id);
  }

  protected Collection<Object> queryByProperties(
      String partitioningPropertyName,
      final Map<String, Object> propertyNameValueMap,
      final Integer firstResult, final Integer maxResults,
      final boolean justCount) {
    final EntityIndexConfig entityIndexConfig = resolveEntityIndexConfig(partitioningPropertyName);
    Session session = createSessionForIndex(config, entityIndexConfig, propertyNameValueMap.get(partitioningPropertyName));

    final Map<String, Entry<EntityIndexConfig, Object>> propertyNameEntityIndexConfigValueMap = createPropertyNameToValueMap(propertyNameValueMap);

    QueryCallback query;

    if (Filter.isMatch(new Predicate<String>() {
      public boolean f(String propertyName) {
        return isPrimitiveCollection(propertyName);
      }
    }, propertyNameEntityIndexConfigValueMap.keySet()))
      query = new QueryCallback() {
        public Collection<Object> execute(Session session) {
          Map<String, Object> revisedPropertyNameValueMap = Transform.toMap(
              new Unary<Entry<String, Entry<EntityIndexConfig, Object>>, String>() {
                public String f(Map.Entry<String, Map.Entry<EntityIndexConfig, Object>> item) {
                  return item.getKey();
                }
              },
              new Unary<Entry<String, Entry<EntityIndexConfig, Object>>, Object>() {
                public Object f(Map.Entry<String, Map.Entry<EntityIndexConfig, Object>> item) {
                  return item.getValue().getValue();
                }
              },
              propertyNameEntityIndexConfigValueMap.entrySet());

          return justCount
              ? queryWithHQLRowCount(session, revisedPropertyNameValueMap, firstResult, maxResults)
              : queryWithHQL(session, revisedPropertyNameValueMap, firstResult, maxResults);
        }
      };
    else
      query = new QueryCallback() {
        @SuppressWarnings("unchecked")
        public Collection<Object> execute(Session session) {
          Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
          for (Entry<EntityIndexConfig, Object> entityIndexConfigValueEntry : propertyNameEntityIndexConfigValueMap.values()) {
            EntityIndexConfig entityIndexConfig = entityIndexConfigValueEntry.getKey();
            Object value = entityIndexConfigValueEntry.getValue();
            addPropertyRestriction(entityIndexConfig, criteria, entityIndexConfig.getPropertyName(), value);
          }
          addPaging(firstResult, maxResults, criteria);
          if (justCount)
            criteria.setProjection(Projections.rowCount());
          return criteria.list();
        }
      };
    return queryInTransaction(query, session);
  }

  private Map<String, Entry<EntityIndexConfig, Object>> createPropertyNameToValueMap(
      final Map<String, Object> propertyNameValueMap) {
    return
        Transform.toOrderedMap(
            new Unary<String, Entry<String, Entry<EntityIndexConfig, Object>>>() {
              public Entry<String, Entry<EntityIndexConfig, Object>> f(String propertyName) {
                EntityIndexConfig entityIndexConfig = resolveEntityIndexConfig(propertyName);
                DataIndexDelegate dataIndexDelegate = AnnotationHelper.getAnnotationDeeply(clazz, propertyName, DataIndexDelegate.class);
                EntityIndexConfig resolvedEntityIndexConfig = (dataIndexDelegate != null)
                    ? resolveEntityIndexConfig(dataIndexDelegate.value())
                    : entityIndexConfig;
                return new Pair<String, Entry<EntityIndexConfig, Object>>(
                    resolvedEntityIndexConfig.getPropertyName(),
                    new Pair<EntityIndexConfig, Object>(resolvedEntityIndexConfig, propertyNameValueMap.get(propertyName)));
              }
            }, propertyNameValueMap.keySet());
  }


  @SuppressWarnings("unchecked")
  protected Collection<Object> queryWithHQL(Session session, Map<String, Object> propertyNameValueMap, Integer firstResult, Integer maxResults) {
    String queryString = createHQLQuery(propertyNameValueMap);
    Query query = session.createQuery(queryString);
    if (maxResults != 0) {
      query.setFirstResult(firstResult);
      query.setMaxResults(maxResults);
    }
    for (Entry<String, Object> entry : propertyNameValueMap.entrySet())
      query.setParameter(entry.getKey(), entry.getValue());
    return query.list();
  }

  @SuppressWarnings("unchecked")
  protected Collection queryWithHQLRowCount(Session session, Map<String, Object> propertyNameValueMap, Integer firstResult, Integer maxResults) {
    String queryString = String.format("select count(%s) %s",
        config.getIdPropertyName(),
        createHQLQuery(propertyNameValueMap));

    Query query = session.createQuery(queryString);
    if (maxResults != 0) {
      query.setFirstResult(firstResult);
      query.setMaxResults(maxResults);
    }
    for (Entry<String, Object> entry : propertyNameValueMap.entrySet())
      query.setParameter(entry.getKey(), entry.getValue());
    return Transform.map(new Unary<Long, Integer>() {
      public Integer f(Long item) {
        return item.intValue();
      }
    }, query.list());
  }

  private String createHQLQuery(Map<String, Object> propertyNameValueMap) {
    return String.format("from %s as x where", GeneratedClassFactory.getGeneratedClass(config.getRepresentedInterface()).getSimpleName())
        + Amass.join(
        new Joiner<Entry<String, Object>, String>() {
          @Override
          public String f(Entry<String, Object> entry, String result) {
            return result + " and " + toHql(entry);
          }
        },
        new Unary<Entry<String, Object>, String>() {
          public String f(Entry<String, Object> entry) {
            return toHql(entry);
          }
        },
        propertyNameValueMap.entrySet());
  }

  private String toHql(Entry<String, Object> entry) {
    String propertyName = entry.getKey();
    if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName))
      return String.format(" :%s in elements (x.%s)", propertyName, propertyName);
    else
      return String.format(" :%s = x.%s", propertyName, propertyName);
  }


  private static int getSaveChunkSize() {
    return BaseDataAccessObject.CHUNK_SIZE;
  }

  public synchronized static void setSaveChunkSize(int size) {
    BaseDataAccessObject.CHUNK_SIZE = size;
  }

  @SuppressWarnings("unchecked")
  public Class<Object> getRespresentedClass() {
    return (Class<Object>) EntityResolver.getPersistedImplementation(clazz);
  }

  public static void doInTransaction(SessionCallback callback, Session session) {
    Transaction tx = null;
    try {
      tx = session.beginTransaction();
      callback.execute(session);
      tx.commit();
    } catch (RuntimeException e) {
      LogFactory.getLog(BaseDataAccessObject.class).error("doInTransaction: Error on data node " + RecordNodeOpenSessionEvent.getNode(), e);
      if (tx != null)
        tx.rollback();
      throw e;
    } finally {
      session.close();
    }
  }

  public static Collection<Object> queryInTransaction(QueryCallback callback, Session session) {
    Collection<Object> results = Lists.newArrayList();
    try {
      session.setFlushMode(FlushMode.MANUAL);
      Transaction tx = session.beginTransaction();
      results = callback.execute(session);
      tx.commit();
    } catch (RuntimeException e) {
      LogFactory.getLog(BaseDataAccessObject.class).error("queryInTransaction: Error on data node " + RecordNodeOpenSessionEvent.getNode(), e);
      throw e;
    } finally {
      session.close();
    }
    return results;
  }

  public Collection<Object> populateDataIndexDelegates(Collection<Object> instances) {
    return Transform.map(new Unary<Object, Object>() {
      @SuppressWarnings("unchecked")
      public Object f(final Object instance) {
        final List<Method> allMethodsWithAnnotation = AnnotationHelper.getAllMethodsWithAnnotation(clazz, DataIndexDelegate.class);
        if (allMethodsWithAnnotation.size() == 0)
          return instance;
        Object modified = new GenerateInstance<Object>((Class<Object>) clazz).generateAndCopyProperties(instance);
        for (Method getter : allMethodsWithAnnotation) {
          String delegatorPropertyName = ReflectionTools.getPropertyNameOfAccessor(getter);
          EntityIndexConfig entityIndexConfig = config.getEntityIndexConfig(delegatorPropertyName);
          String delegatePropertyName = AnnotationHelper.getAnnotationDeeply(clazz, delegatorPropertyName, DataIndexDelegate.class).value();
          GeneratedInstanceInterceptor.setProperty(
              modified,
              delegatePropertyName,
              Filter.grepUnique(entityIndexConfig.getIndexValues(modified)));
        }
        return modified;
      }
    }, instances);
  }

  private boolean isPrimitiveCollection(final String propertyName) {
    return ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
        && !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName);
  }

  protected EntityIndexConfig resolveEntityIndexConfig(String propertyName) {
    return config.getPrimaryIndexKeyPropertyName().equals(propertyName)
        ? createEntityIndexConfigForPartitionIndex(config)
        : config.getEntityIndexConfig(propertyName);
  }

  private EntityIndexConfig createEntityIndexConfigForPartitionIndex(EntityConfig entityConfig) {
    if (partitionIndexEntityIndexConfig == null)
      partitionIndexEntityIndexConfig = new EntityIndexConfigImpl(entityConfig.getRepresentedInterface(), entityConfig.getPrimaryIndexKeyPropertyName());
    return partitionIndexEntityIndexConfig;
  }

  private void addPropertyRestriction(EntityIndexConfig indexConfig, Criteria criteria, String propertyName, Object propertyValue) {
    if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName))
      if (ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
        criteria.createAlias(propertyName, "x")
            .add(Restrictions.eq("x." + indexConfig.getInnerClassPropertyName(), propertyValue));
      } else
        throw new UnsupportedOperationException("This call should have used HQL, not Criteria");
    else
      criteria.add(Restrictions.eq(propertyName, propertyValue));
  }

  protected Session createSessionForIndex(EntityConfig entityConfig, EntityIndexConfig indexConfig, Object propertyValue) {
    if (indexConfig.getIndexType().equals(IndexType.Delegates))
      return factory.openSession(
          ((EntityIndexConfigDelegator) indexConfig).getDelegateEntityConfig().getResourceName(),
          propertyValue);
    else if (indexConfig.getIndexType().equals(IndexType.Hive))
      return factory.openSession(
          entityConfig.getResourceName(),
          indexConfig.getIndexName(),
          propertyValue);
    else if (indexConfig.getIndexType().equals(IndexType.Data))
      return factory.openAllShardsSession();
    else if (indexConfig.getIndexType().equals(IndexType.Partition))
      return factory.openSession(propertyValue);
    throw new RuntimeException(String.format("Unknown IndexType: %s", indexConfig.getIndexType()));
  }

  private void addPropertyRangeRestriction(Criteria criteria, String propertyName, Object minValue, Object maxValue) {
    if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName))
      if (ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
        criteria.createAlias(propertyName, "x")
            .add(Restrictions.between("x." + propertyName, minValue, maxValue));
      } else
        throw new UnsupportedOperationException("This isn't working yet");
    else
      criteria.add(Restrictions.between(propertyName, minValue, maxValue));
  }

  private void doSave(final Object entity, SessionCallback callback, SessionCallback cleanupCallback) {
    try {
      doInTransaction(callback, getSession());
    } catch (org.hibernate.TransactionException dupe) {
      if (dupe.getCause().getClass().equals(org.hibernate.exception.ConstraintViolationException.class)
          && !exists(config.getId(entity))) {
        doInTransaction(cleanupCallback, factory.openSession(config.getPrimaryIndexKey(entity)));
      } else {
        log.error(String.format("Detected an integrity constraint violation on the data node but %s with id %s exists in the directory.", config.getResourceName(), config.getId(entity)));
        throw dupe;
      }
    } catch (org.hibernate.exception.ConstraintViolationException dupe) {
      if (!exists(config.getId(entity))) {
        doInTransaction(cleanupCallback, factory.openSession(config.getPrimaryIndexKey(entity)));
      } else {
        log.error(String.format("Detected an integrity constraint violation on the data node but %s with id %s exists in the directory.", config.getResourceName(), config.getId(entity)));
        throw dupe;
      }
    }
  }

  private void doSaveAll(final Collection<Object> entities, SessionCallback callback, SessionCallback cleanupCallback) {
    try {
      doInTransaction(callback, getSession());
    } catch (org.hibernate.TransactionException dupe) {
      if (dupe.getCause().getClass().equals(org.hibernate.exception.ConstraintViolationException.class)
          || dupe.getCause().getClass().equals(org.hibernate.StaleObjectStateException.class)) {
        doInTransaction(cleanupCallback, factory.openSession(config.getPrimaryIndexKey(Atom.getFirstOrThrow(entities))));
      } else {
        log.error(String.format("Detected an integrity constraint violation on the data node while doing a saveAll with entities of class %s.", config.getResourceName()));
        throw dupe;
      }
    } catch (org.hibernate.exception.ConstraintViolationException dupe) {
      doInTransaction(cleanupCallback, factory.openSession(config.getPrimaryIndexKey(Atom.getFirstOrThrow(entities))));
    }
  }

  public void deleteAll(SessionCallback callback) {
    doInTransaction(callback, getSession());
  }


  private Boolean existsInSession(Session session, Serializable id) {
    return null != session.get(getRespresentedClass(), id);
  }

  private void validateNonNull(final Collection<Object> collection) {
    if (Filter.isMatch(new Filter.NullPredicate<Object>(), collection)) {
      String ids = Amass.joinByToString(new Joiner.ConcatStrings<String>(", "),
          Transform.map(new Unary<Object, String>() {
            public String f(Object item) {
              return item != null ? config.getId(item).toString() : "null";
            }
          }, collection));
      throw new HiveRuntimeException(String.format("Encountered null items in collection: %s", ids));
    }
  }

  protected Session getSession() {
    return factory.openSession(factory.getDefaultInterceptor());
  }

  private void addPaging(final Integer firstResult,
                         final Integer maxResults, Criteria criteria) {
    if (maxResults > 0) {
      criteria.setFirstResult(firstResult);
      criteria.setMaxResults(maxResults);
    }
  }

  /**
   * for debugging only
   */
  public Collection<Object> getAll() {
    QueryCallback query = new QueryCallback() {
      @SuppressWarnings("unchecked")
      public Collection<Object> execute(Session session) {
        Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        return criteria.list();
      }
    };

    return queryInTransaction(query, factory.openAllShardsSession());
  }

  public Collection<Object> queryDataIndex(final String joinTableName, Object primaryIndexKey) {
    QueryCallback query = new QueryCallback() {
      @SuppressWarnings("unchecked")
      public Collection<Object> execute(Session session) {
        SQLQuery query = session.createSQLQuery("select * from " + joinTableName);
        return query.list();
      }
    };

    return queryInTransaction(query, factory.openSession(primaryIndexKey));
  }
}
