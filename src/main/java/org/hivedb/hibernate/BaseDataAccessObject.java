package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.hibernate.Criteria;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.JDBCConnectionException;
import org.hivedb.Hive;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigDelegator;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Transform.IdentityFunction;

public class BaseDataAccessObject implements DataAccessObject<Object, Serializable>{
	protected HiveSessionFactory factory;
	protected EntityConfig config;
	protected Class<?> clazz;
	protected Interceptor defaultInterceptor = EmptyInterceptor.INSTANCE;
	protected Hive hive;

	public Hive getHive() {
		return hive;
	}

	public void setHive(Hive hive) {
		this.hive = hive;
	}

	public BaseDataAccessObject(EntityConfig config, Hive hive, HiveSessionFactory factory) {
		this.clazz = config.getRepresentedInterface();
		this.config = config;
		this.factory = factory;
		this.hive = hive;
		
	}
	
	public BaseDataAccessObject(EntityConfig config, Hive hive, HiveSessionFactory factory, Interceptor interceptor) {
		this(config,hive,factory);
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
		EntityConfig entityConfig = config;
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
		EntityIndexConfig indexConfig = resolveEntityIndexConfig(propertyName);
		Session session = createSessionForIndex(config, indexConfig, propertyValue);
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
			&& !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
				return queryWithHQL(indexConfig, session, propertyValue);
		}
		else {
			// setResultTransformer fixes a Hibernate bug of returning duplicates when joins exist
			Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
			addPropertyRestriction(indexConfig, criteria, propertyName, propertyValue); 
			return listCriteria(session, criteria);
		}
	}
	
	private Collection<Object> queryWithHQL(EntityIndexConfig indexConfig, Session session, Object propertyValue) {
		Query query = session.createQuery(String.format("from %s as x where :value in elements (x.%s)",
			GeneratedInstanceInterceptor.getGeneratedClass(config.getRepresentedInterface()).getSimpleName(),
			indexConfig.getIndexName())
			).setParameter("value", propertyValue);
		return query.list();
	}

	public Integer getCount(String propertyName, Object propertyValue) {
		EntityIndexConfig indexConfig = resolveEntityIndexConfig(propertyName);
		Session session = createSessionForIndex(config, indexConfig, propertyValue);
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
			&& !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
				return queryWithHQLRowCount(indexConfig, session, propertyValue);
		}
		else {
			// setResultTransformer fixes a Hibernate bug of returning duplicates when joins exist
			Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
			addPropertyRestriction(indexConfig, criteria, propertyName, propertyValue); 
			criteria.setProjection( Projections.rowCount() );
			return (Integer)Atom.getFirstOrThrow(listCriteria(session, criteria));
		}
	}
	
	private Integer queryWithHQLRowCount(EntityIndexConfig indexConfig, Session session, Object propertyValue) {
		Query query = session.createQuery(String.format("select count(%s) from %s as x where :value in elements (x.%s)",
			config.getIdPropertyName(),
			GeneratedInstanceInterceptor.getGeneratedClass(config.getRepresentedInterface()).getSimpleName(),
			indexConfig.getIndexName())
			).setParameter("value", propertyValue);
		return (Integer) Atom.getFirstOrThrow(query.list());
	}
	
	private EntityIndexConfig resolveEntityIndexConfig(String propertyName) {
		EntityIndexConfig indexConfig = config.getPrimaryIndexKeyPropertyName().equals(propertyName)
			? createEntityIndexConfigForPartitionIndex(config)
			: config.getEntityIndexConfig(propertyName);
		return indexConfig;
	}

	private EntityIndexConfig partitionIndexEntityIndexConfig;
	private EntityIndexConfig createEntityIndexConfigForPartitionIndex(EntityConfig entityConfig) {
		if (partitionIndexEntityIndexConfig == null)
			partitionIndexEntityIndexConfig = new EntityIndexConfigImpl(entityConfig.getRepresentedInterface(), entityConfig.getPrimaryIndexKeyPropertyName());
		return partitionIndexEntityIndexConfig;
	}

	public Collection<Object> findByProperty(String propertyName, Object propertyValue, Integer firstResult, Integer maxResults) {
		EntityConfig entityConfig = config;
		EntityIndexConfig indexConfig = entityConfig.getEntityIndexConfig(propertyName);
		Session session = createSessionForIndex(entityConfig, indexConfig, propertyValue);
		
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
				&& !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
			Query query = session.createQuery(String.format("from %s as x where :value in elements (x.%s) order by x.%s asc limit %s, %s",
				GeneratedInstanceInterceptor.getGeneratedClass(entityConfig.getRepresentedInterface()).getSimpleName(),
				indexConfig.getIndexName(),
				entityConfig.getIdPropertyName(),
				firstResult,
				maxResults)
				).setEntity("value", propertyValue);
			return query.list();
		}
		else {
			Criteria criteria = session.createCriteria(entityConfig.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
			addPropertyRestriction(indexConfig, criteria, propertyName, propertyValue);
			criteria.setFirstResult(firstResult);
			criteria.setMaxResults(maxResults);
			criteria.addOrder(Order.asc(entityConfig.getIdPropertyName()));
			return listCriteria(session, criteria);
		}
	}
	
	private void addPropertyRestriction(EntityIndexConfig indexConfig, Criteria criteria, String propertyName, Object propertyValue) {
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName))
			if (ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
				criteria.createAlias(propertyName, "x")
					.add( Restrictions.eq("x." + indexConfig.getInnerClassPropertyName(), propertyValue));
			}
			else
				throw new UnsupportedOperationException("This call should have used HQL, not Criteria");
		else
			criteria.add( Restrictions.eq(propertyName, propertyValue));
	}
	
	private Session createSessionForIndex(EntityConfig entityConfig, EntityIndexConfig indexConfig, Object propertyValue) {
		if (indexConfig.getIndexType().equals(IndexType.Delegates))
			return factory.openSession(
					((EntityIndexConfigDelegator)indexConfig).getDelegateEntityConfig().getResourceName(),
					propertyValue);
		else if (indexConfig.getIndexType().equals(IndexType.Hive))
			return factory.openSession(
					entityConfig.getResourceName(), 
					indexConfig.getIndexName(), 
					propertyValue);
		else if (indexConfig.getIndexType().equals(IndexType.Data))
			return factory.openAllShardsSession();
		else if (indexConfig.getIndexType().equals(IndexType.Partition))
			return factory.openSession(propertyValue);
		throw new RuntimeException(String.format("Unknown IndexType: %s", indexConfig.getIndexType()));
	}
	
	public Collection<Object> findByPropertyRange(String propertyName, Object minValue, Object maxValue) {
		// Use an AllShardsresolutionStrategy + Criteria
		EntityConfig entityConfig = config;
		EntityIndexConfig indexConfig = config.getEntityIndexConfig(propertyName);
		Session session = factory.openAllShardsSession();
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
				&& !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
			Query query = session.createQuery(String.format("from %s as x where x.%s between (:minValue, :maxValue)",
				entityConfig.getRepresentedInterface().getSimpleName(),
				indexConfig.getIndexName())
				).setEntity("minValue", minValue).setEntity("maxValue", maxValue);
			return query.list();
		}
		else {
			Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
			addPropertyRangeRestriction(indexConfig, criteria, propertyName, minValue, maxValue);
			return listCriteria(session, criteria);
		}
	}
	public Integer getCountByRange(String propertyName, Object minValue, Object maxValue) {
		// Use an AllShardsresolutionStrategy + Criteria
		EntityIndexConfig indexConfig = config.getEntityIndexConfig(propertyName);
		Session session = factory.openAllShardsSession();
		Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
		addPropertyRangeRestriction(indexConfig, criteria, propertyName, minValue, maxValue);
		criteria.setProjection( Projections.rowCount() );
		return (Integer)Atom.getFirstOrThrow(listCriteria(session, criteria));
	}
	
	public Collection<Object> findByPropertyRange(String propertyName, Object minValue, Object maxValue, Integer firstResult, Integer maxResults) {
		// Use an AllShardsresolutionStrategy + Criteria
		EntityConfig entityConfig = config;
		EntityIndexConfig indexConfig = config.getEntityIndexConfig(propertyName);
		Session session = factory.openAllShardsSession();
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName)
				&& !ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
			Query query = session.createQuery(String.format("from %s as x where %s between (:minValue, :maxValue) order by x.%s asc limit %s, %s",
				entityConfig.getRepresentedInterface().getSimpleName(),
				indexConfig.getIndexName(),
				entityConfig.getIdPropertyName(),
				firstResult,
				maxResults)
				).setEntity("minValue", minValue).setEntity("maxValue", maxValue);
			return query.list();
		}
		else {
			Criteria criteria = session.createCriteria(config.getRepresentedInterface()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
			addPropertyRangeRestriction(indexConfig, criteria, propertyName, minValue, maxValue);
			criteria.add( Restrictions.between(propertyName, minValue, maxValue));
			criteria.setFirstResult(firstResult);
			criteria.setMaxResults(maxResults);
			criteria.addOrder(Order.asc(propertyName));
			return listCriteria(session, criteria);
		}
	}
	
	private void addPropertyRangeRestriction(EntityIndexConfig indexConfig, Criteria criteria, String propertyName, Object minValue, Object maxValue) {
		if (ReflectionTools.isCollectionProperty(config.getRepresentedInterface(), propertyName))
			if (ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), propertyName)) {
				criteria.createAlias(propertyName, "x")
					.add(  Restrictions.between("x." + propertyName, minValue, maxValue));
			}
			else
				throw new UnsupportedOperationException("This isn't working yet");
		else
			criteria.add( Restrictions.between(propertyName, minValue, maxValue));
	}
	
	private Collection<Object> listCriteria(Session session, Criteria criteria) {
		try {
			return criteria.list();
		} finally {
			if(session != null)
				session.close();
		}
	}
	
	private Collection<Object> findByProperty(Session session, Query query) {
		try {
			return query.list();
		} finally {
			if(session != null)
				session.close();
		}
	}

	public Object save(final Object entity) {
		// Compensates for Hibernate's inability to delete items orphaned by updates
		deleteOrphanedCollectionItems(Collections.singletonList(entity));
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				session.saveOrUpdate(getRespresentedClass().getName(),entity);
			}};
		doInTransaction(callback, getSession());
		return entity;
	}

	public Collection<Object> saveAll(final Collection<Object> collection) {
		// Compensates for Hibernate's inability to delete items orphaned by updates
		deleteOrphanedCollectionItems(collection);
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {
				for(Object entity : collection) {
					session.saveOrUpdate(getRespresentedClass().getName(), entity);
				}
			}};
		doInTransaction(callback, getSession());
		return collection;
	}
	
	private void deleteOrphanedCollectionItems(final Collection entities) {
		
		final Session getSession = getSession();
		final Map<Object, Object> entityToLoadedEntity = Transform.toMap(
				new IdentityFunction<Object>(),
				new Unary<Object,Object>() {
					public Object f(Object entity) {
						return get(config.getId(entity), getSession);
					}
				},
				entities,
				new Predicate<Map.Entry<Object,Object>>() {
					 public boolean f(Entry<Object, Object> entry) {
						return entry.getValue() != null;
				}});
		getSession.close();
				
		final Session session = getSession();
		SessionCallback callback = new SessionCallback(){
			public void execute(Session session) {	
				for (Object entity : entities ) {
					if (!entityToLoadedEntity.containsKey(entity))
						continue;
					Object loadedEntity = entityToLoadedEntity.get(entity);
					for (EntityIndexConfig entityIndexConfig : Filter.grep(new Predicate<EntityIndexConfig>() {
						public boolean f(EntityIndexConfig entityIndexConfig) {
							return ReflectionTools.isComplexCollectionItemProperty(config.getRepresentedInterface(), entityIndexConfig.getPropertyName());						
						}}, config.getEntityIndexConfigs())) {
						Collection<Object> newIds = entityIndexConfig.getIndexValues(entity);
						String indexItemIdProperty = entityIndexConfig.getInnerClassPropertyName();
						for (Object item : (Collection<Object>)ReflectionTools.invokeGetter(loadedEntity, entityIndexConfig.getPropertyName())) {
							Object itemId = ReflectionTools.invokeGetter(item, indexItemIdProperty);
							if (!newIds.contains(itemId)) {
								session.delete(item);
							}
						}
					}
					// Prevents NonUniqueObjectException on collection items during save-update cascades
					session.evict(loadedEntity);
				}
			}};
		doInTransaction(callback, session);
	}
	
	protected Session getSession() {
		return factory.openSession(factory.getDefaultInterceptor());
	}

	@SuppressWarnings("unchecked")
	public Class<Object> getRespresentedClass() {
		return (Class<Object>) EntityResolver.getPersistedImplementation(clazz);
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
}
