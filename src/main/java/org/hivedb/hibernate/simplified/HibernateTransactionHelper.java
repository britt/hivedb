package org.hivedb.hibernate.simplified;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hivedb.hibernate.QueryCallback;
import org.hivedb.hibernate.RecordNodeOpenSessionEvent;
import org.hivedb.hibernate.SessionCallback;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Atom;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class HibernateTransactionHelper {
  private final static Log log = LogFactory.getLog(HibernateTransactionHelper.class);

  public Object querySingleInTransaction(QueryCallback callback, Session session) {
      Collection<Object> results = Lists.newArrayList();
      try {
        session.setFlushMode(FlushMode.MANUAL);
        Transaction tx = session.beginTransaction();
        results = callback.execute(session);
        tx.commit();
      } catch( RuntimeException e ) {
        log.debug("queryInTransaction: Error on data node " + RecordNodeOpenSessionEvent.getNode(), e);
        throw e;
      } finally {
        session.close();
      }
      return Atom.getFirstOrNull(results);
    }


  public Collection<Object> queryInTransaction(QueryCallback callback, Session session) {
		Collection<Object> results = Lists.newArrayList();
		try {
			session.setFlushMode(FlushMode.MANUAL);
			Transaction tx = session.beginTransaction();
			results = callback.execute(session);
			tx.commit();
		} catch( RuntimeException e ) {
			log.debug("queryInTransaction: Error on data node " + RecordNodeOpenSessionEvent.getNode(), e);
			throw e;
		} finally {
			session.close();
		}
		return results;
	}

  public void updateInTransaction(SessionCallback callback, Session session) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			callback.execute(session);
			tx.commit();
		} catch( RuntimeException e ) {
			log.debug("doInTransaction: Error on data node " + RecordNodeOpenSessionEvent.getNode(), e);
			if(tx != null)
				tx.rollback();
			throw e;
		} finally {
			session.close();
		}
	}

  public<ID extends Serializable> QueryCallback newGetCallback(final ID id, final Class<?> clazz) {
    return new QueryCallback() {
      public Collection<Object> execute(Session session) {
        return Collections.singletonList(session.get(clazz, id));
      }
    };
  }

  public<T> SessionCallback newSaveCallback(final T entity, final Class<T> clazz) {
    return new SessionCallback() {
      public void execute(Session session) {
        session.saveOrUpdate(clazz.getName(), entity);
      }
    };
  }
}


