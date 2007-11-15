package org.hivedb.hibernate;

import java.io.Serializable;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.configuration.EntityHiveConfig;

public class DataAccessObjectFactory<T, ID extends Serializable> {
	
	EntityHiveConfig entityHiveConfig;
	Class<T> representedClass;
	public DataAccessObjectFactory(EntityHiveConfig entityHiveConfig, Class<T> representedClass) {
		this.entityHiveConfig = entityHiveConfig;
		this.representedClass = representedClass;
	}
	public DataAccessObject<T, ID> create() {
		return new BaseDataAccessObject<T,ID>(
				representedClass, 
				entityHiveConfig, 
				new HiveSessionFactoryBuilderImpl(
					entityHiveConfig, 
					new SequentialShardAccessStrategy()));
	}
}
