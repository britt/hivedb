package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;

public class HiveFinder implements Finder {
	private Hive hive;
	
	public HiveFinder(Hive hive) {
		this.hive = hive;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Nameable> T findByName(Class<T> forClass, String name) {
		if(forClass.equals(Hive.class))
			return (T) hive;
		else
			return (T) hive.getPartitionDimension();
	}

	@SuppressWarnings("unchecked")
	public <T extends Nameable> Collection<T> findCollection(Class<T> forClass) {
		if(forClass.equals(Hive.class))
			return (Collection<T>) Arrays.asList(hive);
		else
			return (Collection<T>) Arrays.asList(hive.getPartitionDimension());
	}
}
