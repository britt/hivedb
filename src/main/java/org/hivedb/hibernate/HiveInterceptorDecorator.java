package org.hivedb.hibernate;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.EntityMode;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;

public class HiveInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	
	@SuppressWarnings("unchecked")
	@Override
	public Object instantiate(String entityName, EntityMode entityMode, Serializable id) throws CallbackException {
		Class<?> clazz;
		try {
			clazz = Class.forName(entityName);
		} catch (ClassNotFoundException e) {
			throw new CallbackException(String.format("Unable to load class for %s", entityName), e);
		}
		if(EntityResolver.generatesImplementation(clazz))
			return getInstance(GeneratedInstanceInterceptor.getGeneratedClass(clazz));
		else if(EntityResolver.isGeneratedImplementation(clazz)){
			Class generatingClass = Filter.grepSingle(new Predicate<Class>(){
				public boolean f(Class item) {
					return EntityResolver.generatesImplementation(item);
				}}, Arrays.asList(clazz.getInterfaces()));
			Object instance = getInstance(GeneratedInstanceInterceptor.getGeneratedClass(generatingClass));
			Method idMethod = AnnotationHelper.getFirstMethodWithAnnotation(generatingClass, EntityId.class);
			if(idMethod != null)
				GeneratedInstanceInterceptor.setProperty(instance, BeanUtils.findPropertyForMethod(idMethod).getName(), id);
			return instance;
		}
		else
			return super.instantiate(entityName, entityMode, id);
	}
	
	private Object getInstance(Class<?> clazz) {
		try {
			return clazz.newInstance();
		} catch (InstantiationException e) {
			throw new CallbackException(e);
		} catch (IllegalAccessException e) {
			throw new CallbackException(e);
		} 
	}

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
