package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.HiveException;

public interface Finder {
	<T extends Nameable> T findByName(Class<T> forClass, String name) throws HiveException;
	<T extends Nameable> Collection<T> findCollection(Class<T> forClass);
}
