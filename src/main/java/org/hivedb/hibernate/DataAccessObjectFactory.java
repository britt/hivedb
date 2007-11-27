package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Properties;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.configuration.EntityHiveConfig;

public class DataAccessObjectFactory<T, ID extends Serializable> {
	
	EntityHiveConfig entityHiveConfig;
	Class<T> representedClass;
	public DataAccessObjectFactory(EntityHiveConfig entityHiveConfig, Class<T> representedClass) {
		this.entityHiveConfig = entityHiveConfig;
		this.representedClass = representedClass;
	}
	@SuppressWarnings("unchecked")
	public DataAccessObject<T, ID> create() {
		return (DataAccessObject<T, ID>) new BaseDataAccessObject(
				representedClass, 
				entityHiveConfig, 
				new HiveSessionFactoryBuilderImpl(
					entityHiveConfig, 
					new SequentialShardAccessStrategy()));
	}
	
	@SuppressWarnings("unchecked")
	public DataAccessObject<T, ID> createDynamicDao() {
		Properties p = new Properties();
		p.setProperty("hibernate.default_entity_mode", "dynamic-map");
		return (DataAccessObject<T, ID>) new DynamicMapDataAccessObject(
				representedClass, 
				entityHiveConfig, 
				new HiveSessionFactoryBuilderImpl(
					entityHiveConfig, 
					new SequentialShardAccessStrategy(),
					p));
	}
}
