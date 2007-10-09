package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.Finder;
import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;

public class HiveConfigFinder implements Finder {
	
	
	@SuppressWarnings("unused")
	private HiveConfig hiveConfig;
	private PartitionDimension partitionDimension;
	public HiveConfigFinder(HiveConfig hiveConfig) {
		this.hiveConfig = hiveConfig;
		this.partitionDimension = PartitionDimensionCreator.create(hiveConfig);
	}
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, final String name) {
		
		return (T)partitionDimension;
	}
		
	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		return (Collection<T>) Arrays.asList(partitionDimension);
	}
}
