package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.exception.ConstraintViolationException;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.QueryCallback;
import org.hivedb.hibernate.SessionCallback;
import org.hivedb.util.functional.Atom;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A DataAccessObject for storing and retrieving objects using Hibernate. This class also acts
 * as an error detection and repair mechanism for the directory.  If a directory error is detected
 * SimpleDataAccessObject will attempt to fix it either by re-indexing the object or removing the
 * erroneous directory entry.  Thus many of the operations can produce side-effects. All error
 * correcting operations are marked as such.
 */
public class SimpleDataAccessObject<T, ID extends Serializable> implements DataAccessObject<T, ID> {
  private final static Log log = LogFactory.getLog(SimpleDataAccessObject.class);
  private Hive hive;
  private EntityConfig config;
  private HibernateTransactionHelper transactionHelper = new HibernateTransactionHelper();
  private HiveSessionFactory factory;
  private Class<T> representedClass;

  public SimpleDataAccessObject(Class<T> clazz, EntityConfig config, Hive hive, HiveSessionFactory factory) {
    this.representedClass = clazz;
    this.config = config;
    this.hive = hive;
    this.factory = factory;
  }

  /**
   * Retrieve an entity by id. <em>This method is error correcting and can have side-effects.</em>
   *
   * @param id
   * @return
   */
  public T get(final ID id) {
    QueryCallback query = new QueryCallback() {
      public Collection<Object> execute(Session session) {
        return Collections.singletonList(session.get(getRespresentedClass(), id));
      }
    };

    T fetched = (T) Atom.getFirstOrThrow(transactionHelper.queryInTransaction(query, getSession()));

    if (fetched == null && exists(id))
      removeDirectoryEntry(id);
    return fetched;
  }

  private void removeDirectoryEntry(ID id) {
    try {
      hive.directory().deleteResourceId(config.getResourceName(), id);
    } catch (HiveLockableException e) {
      log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Unable to cleanup record because Hive was read-only.", config.getResourceName(), id));
    }
    log.warn(String.format("%s with id %s exists in the directory but not on the data node.  Directory record removed.", config.getResourceName(), id));
  }

  public T save(final T entity) {
//		final Object populatedEntity = Atom.getFirstOrNull(populateDataIndexDelegates(Collections.singletonList(entity)));
    SessionCallback callback = new SessionCallback() {
      public void execute(Session session) {
        session.saveOrUpdate(getRespresentedClass().getName(), entity);
      }
    };

//		SessionCallback cleanupCallback = new SessionCallback(){
//			public void execute(Session session) {
//				session.refresh(populatedEntity);
//				session.lock(getRespresentedClass().getName(),populatedEntity, LockMode.UPGRADE);
//				session.update(getRespresentedClass().getName(),populatedEntity);
//				log.warn(String.format("%s with id %s exists in the data node but not on the directory. Data node record was updated and re-indexed.", config.getResourceName(), config.getId(populatedEntity)));
//			}};

//		if (partionDimensionKeyHasChanged(entity))
//			delete(config.getId(entity));
//		doSave(populatedEntity, callback, cleanupCallback);
    transactionHelper.updateInTransaction(callback, getSession());
    return entity;
  }

  private Session getSession() {
    return factory.openSession(factory.getDefaultInterceptor());
  }

  public Boolean exists(ID id) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), id);
  }

  public Collection<T> find(Map<String, Object> properties) {
    return null;
  }

  public Collection<T> find(Map<String, Object> properties, Integer offSet, Integer maxResultSetSize) {
    return null;
  }

  public Collection<T> findInRange(String propertyName, Object minValue, Object maxValue) {
    return null;
  }

  public Collection<T> findInRange(String propertyName, Object minValue, Object maxValue, Integer offSet, Integer maxResultSetSize) {
    return null;
  }

  public Integer getCount(Map<String, Object> properties) {
    return null;
  }

  public Integer getCountInRange(String propertyName, Object minValue, Object maxValue) {
    return null;
  }

  public Collection<T> saveAll(Collection<T> ts) {
    return null;
  }

  public ID delete(final ID id) {
    SessionCallback callback = new SessionCallback() {
      public void execute(Session session) {
        Object deleted = session.get(getRespresentedClass(), id);
        session.delete(deleted);
      }
    };
    transactionHelper.updateInTransaction(callback, getSession());
    return id;
  }

  public Class<T> getRespresentedClass() {
    return representedClass;
  }

  // TODO TEST!
  public void safeSave(final T entity, SessionCallback callback, SessionCallback cleanupCallback) {
    try {
      transactionHelper.updateInTransaction(callback, getSession());
    } catch (HibernateException dupe) {
      if (isDuplicateRecordException(dupe,entity) && !exists((ID) config.getId(entity))) {
        transactionHelper.updateInTransaction(cleanupCallback, factory.openSession(config.getPrimaryIndexKey(entity)));
      } else {
        log.error(
          String.format(
            "Detected an integrity constraint violation on the data node but %s with id %s exists in the directory.",
            config.getResourceName(),
            config.getId(entity)));        
        throw dupe;
      }
    }
  }

  private boolean isDuplicateRecordException(HibernateException dupe, T entity) {
    return
      (dupe.getCause().getClass().isAssignableFrom(ConstraintViolationException.class)
        || dupe.getClass().isAssignableFrom(ConstraintViolationException.class))
				&& !exists((ID)config.getId(entity));
  }
}

