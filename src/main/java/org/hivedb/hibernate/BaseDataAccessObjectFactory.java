package org.hivedb.hibernate;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.serialization.BlobbedEntity;
import org.hivedb.util.serialization.BlobbingDelegateDataAccessObject;

public class BaseDataAccessObjectFactory<T, ID extends Serializable> implements DataAccessObjectFactory<T, ID> {
	
	Hive hive;
	EntityHiveConfig entityHiveConfig;
	Class<T> representedClass;
	public BaseDataAccessObjectFactory(EntityHiveConfig entityHiveConfig, Class<T> representedClass, Hive hive) {
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
					new SequentialShardAccessStrategy(),
					Arrays.asList((Class<?>[])new Class[] { BlobbedEntity.class })),
				new DefaultDelegateDataAccessObject());
	}
}
