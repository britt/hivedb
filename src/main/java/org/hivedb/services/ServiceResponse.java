package org.hivedb.services;

import java.util.Collection;

public interface ServiceResponse<T> {
	Collection<ServiceContainer<T>> getContainers();
	Collection<T> getInstances();
}
