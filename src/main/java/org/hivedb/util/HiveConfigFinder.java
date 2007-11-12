package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.HiveConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;

public class HiveConfigFinder implements Finder {
	
	
	@SuppressWarnings("unused")
	private HiveConfig hiveConfig;
	private PartitionDimension partitionDimension;
	public HiveConfigFinder(SingularHiveConfig hiveConfig) {
		this.hiveConfig = hiveConfig;
		this.partitionDimension = PartitionDimensionCreator.create(hiveConfig);
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, final String name) {
		if(forClass.equals(Hive.class))
			return (T) hiveConfig.getHive();
		else
			return (T)partitionDimension;
	}
		
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if(forClass.equals(Hive.class))
			return (Collection<T>) Arrays.asList(hiveConfig.getHive());
		else
			return (Collection<T>) Arrays.asList(partitionDimension);
	}
}
