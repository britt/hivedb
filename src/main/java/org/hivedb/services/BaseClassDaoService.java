package org.hivedb.services;

import org.hibernate.shards.strategy.access.ShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.*;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class BaseClassDaoService<T,ID extends Serializable> implements ClassDaoService<T,ID> {
	protected EntityConfig config;
	protected DataAccessObject<Object, Serializable> dao;
	
	@SuppressWarnings("unchecked")
	public static BaseClassDaoService create(Class<?> clazz, Hive hive, ShardAccessStrategy strategy) {
		ConfigurationReader reader = new ConfigurationReader(clazz);
		List classes = Collections.singletonList(clazz);
		HiveSessionFactory factory = new HiveSessionFactoryBuilderImpl(hive.getUri(), classes, strategy);
		BaseDataAccessObject dao = new BaseDataAccessObject(reader.getEntityConfig(clazz.getName()), hive, factory);
		return new BaseClassDaoService(reader.getEntityConfig(clazz.getName()), dao);
	}
	
	public BaseClassDaoService(EntityConfig config, DataAccessObject<Object, Serializable> dao) {
		this.config = config;
		this.dao = dao;
	}

	@SuppressWarnings("unchecked")
	public ID delete(ID id) {
		return (ID) dao.delete(id);
	}

	public boolean exists(ID id) {
		return dao.exists(id);
	}

	@SuppressWarnings("unchecked")
	public T get(ID id) {
		return (T) dao.get(id);
	}

	@SuppressWarnings("unchecked")
	public Collection<T> getByReference(String property, Object referenceKey) {
		return (Collection<T>) dao.findByProperty(property, referenceKey);
	}

	@SuppressWarnings("unchecked")
	public Collection<T> getByReferenceRange(String property, Object start, Object end) {
		return (Collection<T>) dao.findByPropertyRange(property, start, end);
	}

	public String getPersistedClass() {
		return config.getRepresentedInterface().getName();
	}

	@SuppressWarnings("unchecked")
	public T save(T obj) {
		return (T) dao.save(obj);
	}

	@SuppressWarnings("unchecked")
	public Collection<T> saveAll(Collection<T> instances) {
		return (Collection<T>) dao.saveAll((Collection<Object>)instances);
	}

	//Debugging
	@SuppressWarnings("unchecked")
	public Collection<T> getAll() {
		return (Collection<T>)dao.getAll();
	}
	public Collection<T> queryDataIndex(String joinTableName, Object primaryIndexKey) {
		return (Collection<T>)dao.queryDataIndex(joinTableName, primaryIndexKey);
	}

	public Integer getCountByReference(String property, Object referenceKey) {
		return dao.getCount(property, referenceKey);
	}
}
