package org.hivedb.services;

import java.util.Collection;

public interface ServiceResponse<T,C extends ServiceContainer<T>> {
	Collection<C> getContainers();
}
