package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.Hive;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class HiveInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	private EntityHiveConfig hiveConfig;
	private HiveIndexer indexer;
	
	public HiveInterceptorDecorator(EntityHiveConfig hiveConfig, Hive hive) {
		this(EmptyInterceptor.INSTANCE, hiveConfig, hive);
	}
	
	public HiveInterceptorDecorator(Interceptor interceptor, EntityHiveConfig hiveConfig, Hive hive) {
		super(interceptor);
		this.hiveConfig = hiveConfig;
		this.indexer = new HiveIndexer(hive);
	}

	@Override
	public Boolean isTransient(Object entity) {
		final Class resolvedEntityClass = resolveEntityClass(entity.getClass());
		if (resolvedEntityClass != null)
			return !indexer.exists(this.hiveConfig.getEntityConfig(resolvedEntityClass), entity);
		else
			throw new HiveKeyNotFoundException(String.format("Class %s is not indexed in the Hive.", entity.getClass().getCanonicalName()));
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
		//Read-only checks are implicit in the delete calls
		//We just need to wrap the exception
		try {
			final Class resolvedEntityClass = resolveEntityClass(entity.getClass());
			if (resolvedEntityClass != null)
				indexer.delete(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
		super.onDelete(entity, id, state, propertyNames, types);
	}
	
	@Override
	public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) throws CallbackException {
		insertIndexes(entity, id);
		return super.onSave(entity, id, state, propertyNames, types);
	}
	
	@Override
	public boolean onFlushDirty(Object entity, Serializable id,
			Object[] currentState, Object[] previousState,
			String[] propertyNames, Type[] types) throws CallbackException {
		updateIndexes(entity);
		return super.onFlushDirty(entity, id, currentState, previousState,
				propertyNames, types);
	}
	
	private void updateIndexes(Object entity) {
		try {
			final Class resolvedEntityClass = resolveEntityClass(entity.getClass());
			if (resolvedEntityClass != null)
				indexer.update(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
	}

	private void insertIndexes(Object entity, Serializable id) {
		try {
			final Class resolvedEntityClass = resolveEntityClass(entity.getClass());
			if (resolvedEntityClass != null)
				indexer.insert(hiveConfig.getEntityConfig(resolvedEntityClass), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
	}

	@Override
	public Object getEntity(String entityName, Serializable id) throws CallbackException {
		return super.getEntity(entityName, id);
	}
}
