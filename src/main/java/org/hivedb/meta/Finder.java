package org.hivedb.meta;

import java.util.Collection;

public interface Finder {
	<T extends Nameable> T findByName(Class<T> forClass, String name);
	<T extends Nameable> Collection<? extends T> findCollection(Class<T> forClass);
}
