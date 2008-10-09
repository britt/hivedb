package org.hivedb.hibernate.simplified;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.action.Executable;
import org.hibernate.event.PostUpdateEvent;
import org.hibernate.event.PostUpdateEventListener;
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
 * This is an alternative way of updating the hive indexes after successful
 * transaction completion (instead of using a custom Interceptor) and up
 * for discussion.  Hooked up via org.hibernate.cfg.Configuration.
 * getEventListeners().setPostUpdateEventListeners
 *
 * @author mellwanger
 */
public class PostUpdateEventListenerImpl implements PostUpdateEventListener {
  private static final Logger log = Logger.getLogger(PostUpdateEventListenerImpl.class);
  private final EntityHiveConfig hiveConfig;
  private final HiveIndexer indexer;

  public PostUpdateEventListenerImpl(EntityHiveConfig hiveConfig, Hive hive) {
    this.hiveConfig = hiveConfig;
    indexer = new HiveIndexer(hive);
  }

  public void onPostUpdate(final PostUpdateEvent event) {
    event.getSession().getActionQueue().execute(new Executable() {

      public void afterTransactionCompletion(boolean success) {
        if (success) {
          updateIndexes(event.getEntity());
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

  private void updateIndexes(Object entity) {
    try {
      final Class<?> resolvedEntityClass = resolveEntityClass(entity.getClass());
      if (resolvedEntityClass != null) {
        final EntityConfig entityConfig = hiveConfig.getEntityConfig(entity.getClass());
        if (indexer.idExists(entityConfig, entityConfig.getId(entity)))
          indexer.updatePartitionDimensionIndexIfNeeded(hiveConfig.getEntityConfig(resolvedEntityClass), entity);
        indexer.update(entityConfig, entity);
      }
    } catch (HiveLockableException e) {
      log.warn(e);
    }
  }

}
