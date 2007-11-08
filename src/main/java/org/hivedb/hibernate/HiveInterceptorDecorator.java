package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityHiveConfig;

public class HiveInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	private EntityHiveConfig hiveConfig;
	private HiveIndexer indexer;
	
	public HiveInterceptorDecorator(EntityHiveConfig hiveConfig) {
		super(EmptyInterceptor.INSTANCE);
		this.hiveConfig = hiveConfig;
		this.indexer = new HiveIndexer(hiveConfig.getHive());
	}
	
	public HiveInterceptorDecorator(Interceptor interceptor, EntityHiveConfig hiveConfig) {
		super(interceptor);
		this.hiveConfig = hiveConfig;
		this.indexer = new HiveIndexer(hiveConfig.getHive());
	}

	@Override
	public Boolean isTransient(Object entity) {
		return !indexer.exists(hiveConfig.getEntityConfig(entity.getClass()), entity);
	}

	@Override
	public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) throws CallbackException {
		//Read-only checks are implicit in the delete calls
		//We just need to wrap the exception
		try {
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
			indexer.update(hiveConfig.getEntityConfig(entity.getClass()), entity);
		} catch (HiveReadOnlyException e) {
			throw new CallbackException(e);
		}
	}

	private void insertIndexes(Object entity, Serializable id) {
		try {
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
