package org.hivedb.hibernate.simplified;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.action.Executable;
import org.hibernate.event.PostDeleteEvent;
import org.hibernate.event.PostDeleteEventListener;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.HiveIndexer;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

import java.io.Serializable;

/**
 * This is an alternative way of deleting the hive indexes after successful
 * transaction completion (instead of using a custom Interceptor) and up
 * for discussion. Hooked up via org.hibernate.cfg.Configuration.
 * getEventListeners().setPostDeleteEventListeners
 *
 * @author mellwanger
 */
public class PostDeleteEventListenerImpl implements PostDeleteEventListener {
  private static final Logger log = Logger.getLogger(PostInsertEventListenerImpl.class);
  private final EntityHiveConfig hiveConfig;
  private final HiveIndexer indexer;

  public PostDeleteEventListenerImpl(EntityHiveConfig hiveConfig, Hive hive) {
    this.hiveConfig = hiveConfig;
    indexer = new HiveIndexer(hive);
  }

  public void onPostDelete(final PostDeleteEvent event) {
    event.getSession().getActionQueue().execute(new Executable() {

      public void afterTransactionCompletion(boolean success) {
        if (success) {
          deleteIndexes(event.getEntity());
        }
      }

      public void beforeExecutions() throws HibernateException {
        // TODO Auto-generated method stub

      }

      public void execute() throws HibernateException {
        // TODO Auto-generated method stub

      }

      public Serializable[] getPropertySpaces() {
        // TODO Auto-generated method stub
        return null;
      }

      public boolean hasAfterTransactionCompletion() {
        return true;
      }

    });

  }

  @SuppressWarnings("unchecked")
  private Class resolveEntityClass(Class clazz) {
    return ReflectionTools.whichIsImplemented(
      clazz,
      Transform.map(new Unary<EntityConfig, Class>() {
        public Class f(EntityConfig entityConfig) {
          return entityConfig.getRepresentedInterface();
        }
      },
        hiveConfig.getEntityConfigs()));
  }

  private void deleteIndexes(Object entity) {
    try {
      final Class<?> resolvedEntityClass = resolveEntityClass(entity.getClass());
      if (resolvedEntityClass != null)
        indexer.delete(hiveConfig.getEntityConfig(entity.getClass()), entity);
    } catch (HiveLockableException e) {
      log.warn(e);
    }
  }

}
