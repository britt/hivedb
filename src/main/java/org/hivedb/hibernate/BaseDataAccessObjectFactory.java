package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Collection;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;

public class BaseDataAccessObjectFactory<T, ID extends Serializable> implements DataAccessObjectFactory<T, ID> {
	
	Hive hive;
	EntityHiveConfig entityHiveConfig;
	Collection<Class<?>> mappedClasses;
	Class<T> representedClass;
	public BaseDataAccessObjectFactory(EntityHiveConfig entityHiveConfig, Collection<Class<?>> mappedClasses, Class<T> representedClass, Hive hive) {
		this.entityHiveConfig = entityHiveConfig;
		this.mappedClasses = mappedClasses;
		this.representedClass = representedClass;
		this.hive = hive;
	}
	@SuppressWarnings("unchecked")
	public DataAccessObject<T, ID> create() {
		return (DataAccessObject<T, ID>) new BaseDataAccessObject( 
				entityHiveConfig.getEntityConfig(representedClass), 
				hive,
				new HiveSessionFactoryBuilderImpl(
					entityHiveConfig, 
					mappedClasses,
					hive,
					new SequentialShardAccessStrategy()));
	}
}
