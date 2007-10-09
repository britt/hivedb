package org.hivedb.util;

import java.util.Collection;

public interface InstanceCollectionValueGetter {
	Collection<Object> get(Object instance);
}
