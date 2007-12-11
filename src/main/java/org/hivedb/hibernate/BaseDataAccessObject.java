package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;

import org.hibernate.Criteria;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hivedb.Hive;
import org.hibernate.exception.JDBCConnectionException;
import org.hivedb.HiveKeyNotFoundException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.util.Lists;

public class BaseDataAccessObject implements DataAccessObject<Object, Serializable>{
	private HiveSessionFactory factory;
	private EntityHiveConfig config;
	private Class<?> clazz;
	private Interceptor defaultInterceptor = EmptyInterceptor.INSTANCE;
	private Hive hive;

	public Hive getHive() {
		return hive;
	}

	public void setHive(Hive hive) {
		this.hive = hive;
	}

	public BaseDataAccessObject(Class<?> clazz, EntityHiveConfig config, Hive hive, HiveSessionFactory factory) {
		this.clazz = clazz;
		this.config = config;
		this.factory = factory;
		this.hive = hive;
		
	}
	
	public BaseDataAccessObject(Class<?> clazz, EntityHiveConfig config, Hive hive, HiveSessionFactory factory, Interceptor interceptor) {
		this(clazz,config,hive,factory);
		this.defaultInterceptor = interceptor;
	}
	
	public Serializable delete(final Serializable id) {
		SessionCallback callback = new SessionCallback() {
			public void execute(Session session) {
				Object deleted = get(id, session);
				session.delete(deleted);
			}};
		doInTransaction(callback, getSession());
		return id;
	}

	public Boolean exists(Serializable id) {
		EntityConfig entityConfig = config.getEntityConfig(getRespresentedClass());
		return hive.directory().doesResourceIdExist(entityConfig.getResourceName(), id);
	}

	public Object get(final Serializable id) {
		Object entity = null;
		Session session = null;
		try {
			session = getSession();
			return get(id, session);
		} finally {
			if(session != null)
				session.close();
		}
	}
	
	private Object get(Serializable id, Session session) {
		Object fetched = session.get(getRespresentedClass(), id);
		return fetched;
	}
	
	@SuppressWarnings("unchecked")
	public Collection<Object> findByProperty(String propertyName, Object propertyValue) {
		EntityConfig entityConfig = config.getEntityConfig(getRespresentedClass());
		EntityIndexConfig indexConfig = getIndexConfig(propertyName, entityConfig.getEntitySecondaryIndexConfigs());
		Collection<Object> entities = Lists.newArrayList();
		Session session = 
			factory.openSession(
				entityConfig.getResourceName(), 
				indexConfig.getIndexName(), 
				propertyValue);
		try {
			Criteria c = session.createCriteria(entityConfig.getRepresentedInterface());
			c.add( Restrictions.eq(indexConfig.getPropertyName(), propertyValue));
			return c.list();
		} finally {
			if(session != null)
				session.close();
		}
	}

	public Object save(final Object entity) {
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(getRespresentedClass().getName(),entity);
			}};
		doInTransaction(callback, getSession());
		return entity;
	}

	public Collection<Object> saveAll(final Collection<Object> collection) {
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				for(Object entity : collection) 
					session.saveOrUpdate(getRespresentedClass().getName(), entity);
			}};
		doInTransaction(callback, getSession());
		return collection;
	}
	
	protected Session getSession() {
		return factory.openSession(factory.getDefaultInterceptor());
	}

	@SuppressWarnings("unchecked")
	public Class<Object> getRespresentedClass() {
		return (Class<Object>) EntityResolver.getMappedClass(clazz);
	}
	
	private EntityIndexConfig getIndexConfig(String name, Collection<? extends EntityIndexConfig> configs) {
		for(EntityIndexConfig cfg : configs)
			if(cfg.getPropertyName().matches(name))
				return cfg;
		throw new HiveKeyNotFoundException(String.format("Could not find index configuration for %s", name), name);
	}
	
	public Interceptor getInterceptor() {return this.defaultInterceptor;}
	
	public void setInterceptor(Interceptor i) {this.defaultInterceptor = i;}
	
	public static void doInTransaction(SessionCallback callback, Session session) {
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			callback.execute(session);
			tx.commit();
		} catch (JDBCConnectionException ce) {
			throw new RuntimeException(ce);
		} catch( RuntimeException e ) {
			if(tx != null)
				try {
					tx.rollback();
				}
				catch (Exception ex){
					throw e;
				}
			throw e;
		} finally {
			session.close();
		}
	}

	public Collection<Object> findByPropertyRange(String propertyName, java.lang.Object minValue, java.lang.Object maxValue) {
		throw new UnsupportedOperationException("Not implemented");
	}
}
