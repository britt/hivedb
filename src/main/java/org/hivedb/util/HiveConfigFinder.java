package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;

public class HiveConfigFinder implements Finder {
	
	
	@SuppressWarnings("unused")
	private EntityHiveConfig entityHiveConfig;
	private PartitionDimension partitionDimension;
	public HiveConfigFinder(EntityHiveConfig entityHiveConfig) {
		this.entityHiveConfig = entityHiveConfig;
		this.partitionDimension = PartitionDimensionCreator.create(entityHiveConfig);
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, final String name) {
		if(forClass.equals(Hive.class))
			return (T) entityHiveConfig.getHive();
		else
			return (T)partitionDimension;
	}
		
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if(forClass.equals(Hive.class))
			return (Collection<T>) Arrays.asList(entityHiveConfig.getHive());
		else
			return (Collection<T>) Arrays.asList(partitionDimension);
	}
}
