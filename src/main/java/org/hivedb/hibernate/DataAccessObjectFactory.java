package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;

public class DataAccessObjectFactory<T, ID extends Serializable> {
	Hive hive;
	EntityHiveConfig entityHiveConfig;
	Class<T> representedClass;
	public DataAccessObjectFactory(EntityHiveConfig entityHiveConfig, Class<T> representedClass, Hive hive) {
		this.entityHiveConfig = entityHiveConfig;
		this.representedClass = representedClass;
		this.hive = hive;
	}
	@SuppressWarnings("unchecked")
	public DataAccessObject<T, ID> create() {
		return (DataAccessObject<T, ID>) new BaseDataAccessObject(
				representedClass, 
				entityHiveConfig, 
				hive,
				new HiveSessionFactoryBuilderImpl(
					entityHiveConfig, 
					hive,
					new SequentialShardAccessStrategy()));
	}
}
