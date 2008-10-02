package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.QueryCallback;
import org.hivedb.hibernate.SessionCallback;
import org.hivedb.hibernate.simplified.session.HiveCriteria;
import org.hivedb.hibernate.simplified.session.HiveCriteriaImpl;
import org.hivedb.hibernate.simplified.session.HiveSessionFactory;
import org.hivedb.util.classgen.ReflectionTools;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * A DataAccessObject for storing and retrieving objects using Hibernate. This class also acts
 * as an error detection and repair mechanism for the directory.  If a directory error is detected
 * SimpleDataAccessObject will attempt to fix it either by re-indexing the object or removing the
 * erroneous directory entry.  Thus many of the operations can produce side-effects. All error
 * correcting operations are marked as such.
 */

// Todo Identify Extractions that can DRY up this code.
// Todo queries
// No HQL -- use Criteria
// Drop primitive collection property query support
public class ErrorCorrectingDataAccessObject<T, ID extends Serializable> implements DataAccessObject<T, ID> {
  private final static Log log = LogFactory.getLog(ErrorCorrectingDataAccessObject.class);
  private Hive hive;
  private EntityConfig config;
  private HibernateTransactionHelper transactionHelper = new HibernateTransactionHelper();
  private HiveSessionFactory factory;
  private Class<T> representedClass;

  public ErrorCorrectingDataAccessObject(Class<T> clazz, EntityConfig config, Hive hive, HiveSessionFactory factory) {
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
    QueryCallback query = transactionHelper.newGetCallback(id, getRespresentedClass());
    T fetched = (T) transactionHelper.querySingleInTransaction(query, factory.openSession());

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
    SessionCallback callback = transactionHelper.newSaveCallback(entity, getRespresentedClass());

		SessionCallback cleanupCallback = new SessionCallback(){
		  public void execute(Session session) {
			  session.refresh(entity);
				session.lock(getRespresentedClass().getName(),entity, LockMode.UPGRADE);
				session.update(getRespresentedClass().getName(),entity);
				log.warn(String.format("%s with id %s exists in the data node but not on the directory. Data node record was updated and re-indexed.", config.getResourceName(), config.getId(entity)));
			}
    };

		if (hasPartitionDimensionKeyChanged(entity))
			delete((ID) config.getId(entity));
    
    try {
      transactionHelper.updateInTransaction(callback, factory.openSession());
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
    
    return entity;
  }

  public Collection<T> saveAll(Collection<T> entities) {
    for(T t : entities)
      save(t);
    return entities;
  }
  
  public ID delete(final ID id) {
    SessionCallback callback = new SessionCallback() {
      public void execute(Session session) {
        Object deleted = session.get(getRespresentedClass(), id);
        session.delete(deleted);
      }
    };
    if(exists(id))
      transactionHelper.updateInTransaction(callback, factory.openSession());
    return id;
  }
 
	public boolean hasPartitionDimensionKeyChanged(Object entity) {
		return hive.directory().doesResourceIdExist(config.getResourceName(), config.getId(entity)) &&
				!config.getPrimaryIndexKey(entity).equals(hive.directory().getPrimaryIndexKeyOfResourceId(config.getResourceName(), config.getId(entity)));
	}

  public Boolean exists(ID id) {
    return hive.directory().doesResourceIdExist(config.getResourceName(), id);
  }

  public Collection<T> find(Map<String, Object> properties) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Collection<T> find(Map<String, Object> properties, Integer offSet, Integer maxResultSetSize) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Collection<T> findInRange(final String propertyName, final Object minValue, final Object maxValue) {
    checkForHiveIndexedProperty(propertyName);

    QueryCallback query = new QueryCallback(){
      public Collection<Object> execute(Session session) {
        HiveCriteria c = new HiveCriteriaImpl(session.createCriteria(getRespresentedClass()), getRespresentedClass());
        if(ReflectionTools.isComplexCollectionItemProperty(getRespresentedClass(), propertyName)){
          c.createCriteria(propertyName).add(  Restrictions.between("id", minValue, maxValue));
        } else {
          c.add(Restrictions.between(propertyName, minValue, maxValue));                                  
        }
        return c.list();
      }
    };
    return (Collection<T>) transactionHelper.queryInTransaction(query, factory.openSession());
  }

  public Collection<T> findInRange(final String propertyName, final Object minValue, final Object maxValue, final Integer offSet, final Integer maxResultSetSize) {
    checkForHiveIndexedProperty(propertyName);

    QueryCallback query = new QueryCallback() {
      public Collection<Object> execute(Session session) {
        HiveCriteria c = new HiveCriteriaImpl(session.createCriteria(getRespresentedClass()), getRespresentedClass());
        if (ReflectionTools.isComplexCollectionItemProperty(getRespresentedClass(), propertyName)) {
          c.createCriteria(propertyName)
            .add(Restrictions.between("id", minValue, maxValue));
        } else {
          c.add(Restrictions.between(propertyName, minValue, maxValue));
        }
        c.setFirstResult(offSet);
        c.setMaxResults(maxResultSetSize);
        return c.list();
      }
    };
    return (Collection<T>) transactionHelper.queryInTransaction(query, factory.openSession());
  }

  private void checkForHiveIndexedProperty(String propertyName) throws UnsupportedOperationException {
    if(config.getEntityIndexConfig(propertyName) == null)
      throw new UnsupportedOperationException(String.format("%s.%s is not indexed by the Hive. This operation can only be performed on indexed properties.",getRespresentedClass().getSimpleName(), propertyName));
  }

  public Integer getCount(Map<String, Object> properties) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Integer getCountInRange(final String propertyName, final Object minValue, final Object maxValue) {
    checkForHiveIndexedProperty(propertyName);

    QueryCallback query = new QueryCallback(){
      public Collection<Object> execute(Session session) {
        HiveCriteria c = new HiveCriteriaImpl(session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY), getRespresentedClass());
        if (ReflectionTools.isComplexCollectionItemProperty(getRespresentedClass(), propertyName)) {
          c.createCriteria(propertyName)
            .add(Restrictions.between("id", minValue, maxValue));
        } else {
          c.add(Restrictions.between(propertyName, minValue, maxValue));
        }
				c.setProjection( Projections.rowCount() );
				return c.list();
      }
    };
    
    return (Integer) transactionHelper.querySingleInTransaction(query, factory.openSession());
  }

  public Class<T> getRespresentedClass() {
    return representedClass;
  }

  private boolean isDuplicateRecordException(HibernateException dupe, T entity) {
    try {
      return
        (dupe.getCause().getClass().isAssignableFrom(ConstraintViolationException.class)
          || dupe.getClass().isAssignableFrom(ConstraintViolationException.class))
				  && !exists((ID)config.getId(entity));
    } catch(RuntimeException e) {
      return false;
    }
  }
}

