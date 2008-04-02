package org.hivedb.hibernate;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.EntityMode;
import org.hibernate.Interceptor;
import org.hibernate.shards.util.InterceptorDecorator;
import org.hibernate.type.Type;
import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.HiveLockableException;
import org.hivedb.annotations.AnnotationHelper;
import org.hivedb.annotations.EntityId;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GeneratedClassFactory;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.beans.BeanUtils;

public class HiveInterceptorDecorator extends InterceptorDecorator implements Interceptor {
	private EntityHiveConfig hiveConfig;
	private HiveIndexer indexer;
	
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
			return getInstance(GeneratedClassFactory.getGeneratedClass(clazz));
		else if(EntityResolver.isGeneratedImplementation(clazz)){
			Class generatingClass = Filter.grepSingle(new Predicate<Class>(){
				public boolean f(Class item) {
					return EntityResolver.generatesImplementation(item);
				}}, Arrays.asList(clazz.getInterfaces()));
			Object instance = getInstance(GeneratedClassFactory.getGeneratedClass(generatingClass));
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
				if(indexer.exists(hiveConfig.getEntityConfig(resolvedClass), entity))
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
			if (resolvedEntityClass != null)
				indexer.update(hiveConfig.getEntityConfig(entity.getClass()), entity);
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
