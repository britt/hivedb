package org.hivedb.hibernate.simplified;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.HiveFacade;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.EntityResolver;
import org.hivedb.hibernate.HiveIndexer;
import org.hivedb.util.classgen.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

import java.io.Serializable;
import java.util.Iterator;

public class HiveInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	private EntityHiveConfig hiveConfig;
	private HiveIndexer indexer;

	public HiveInterceptorDecorator(EntityHiveConfig hiveConfig, HiveFacade hive) {
		this(EmptyInterceptor.INSTANCE, hiveConfig, hive);
	}

	public HiveInterceptorDecorator(Interceptor interceptor, EntityHiveConfig hiveConfig, HiveFacade hive) {
		super(interceptor);
		this.hiveConfig = hiveConfig;
		this.indexer = new HiveIndexer(hive);
	}

	@Override
	public Boolean isTransient(Object entity) {
		Class<?> clazz = new EntityResolver(hiveConfig).resolveEntityInterface(entity.getClass());
		if (clazz != null)
			return !indexer.exists(this.hiveConfig.getEntityConfig(clazz), entity);
		return super.isTransient(entity);
	}

	@SuppressWarnings("unchecked")
	private Class resolveEntityClass(Class clazz) {
		return ReflectionTools.whichIsImplemented(
				clazz,
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					hiveConfig.getEntityConfigs()));
	}

	@Override
	public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) throws CallbackException {
		try {
			final Class<?> resolvedEntityClass = resolveEntityClass(entity.getClass());
			if (resolvedEntityClass != null)
				indexer.delete(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveLockableException e) {
			throw new CallbackException(e);
		}
		super.onDelete(entity, id, state, propertyNames, types);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void postFlush(Iterator entities) throws CallbackException {
		while(entities.hasNext()) {
			Object entity = entities.next();
			Class<?> resolvedClass = resolveEntityClass(entity.getClass());
			if(resolvedClass != null) {
				final EntityConfig entityConfig = hiveConfig.getEntityConfig(resolvedClass);
				if(indexer.exists(entityConfig, entity))
					updateIndexes(entity);
				else
					insertIndexes(entity);
			}
    }
		super.postFlush(entities);
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
			throw new CallbackException(e);
		}
	}

	private void insertIndexes(Object entity) {
		try {
			final Class<?> resolvedEntityClass = resolveEntityClass(entity.getClass());
			if (resolvedEntityClass != null)
				indexer.insert(hiveConfig.getEntityConfig(resolvedEntityClass), entity);
		} catch (HiveLockableException e) {
			throw new CallbackException(e);
		}
	}
}
