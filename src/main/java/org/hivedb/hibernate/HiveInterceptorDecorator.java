package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.Hive;
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
		final EntityConfig resolvedEntityConfig = resolveEntityConfig(entity.getClass());
		return resolvedEntityConfig != null
			? !indexer.exists(resolvedEntityConfig, entity)
			: super.isTransient(entity);
	}

	@SuppressWarnings("unchecked")
	private EntityConfig resolveEntityConfig(Class clazz) {
		return hiveConfig.getEntityConfig(ReflectionTools.whichIsImplemented(
				clazz, 
				Transform.map(new Unary<EntityConfig, Class>() {
					public Class f(EntityConfig entityConfig) {
						return entityConfig.getRepresentedInterface();
					}},
					hiveConfig.getEntityConfigs())));
	}

	private boolean isHiveEntity(Object entity) {
		return hiveConfig.getEntityConfig(entity.getClass()) != null;
	}

	@Override
	public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) throws CallbackException {
		//Read-only checks are implicit in the delete calls
		//We just need to wrap the exception
		try {
			if (isHiveEntity(entity))
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
			if (isHiveEntity(entity))
				indexer.update(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
	}

	private void insertIndexes(Object entity, Serializable id) {
		try {
			if (isHiveEntity(entity))
				indexer.insert(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
	}

	@Override
	public Object getEntity(String entityName, Serializable id) throws CallbackException {
		return super.getEntity(entityName, id);
	}
}
