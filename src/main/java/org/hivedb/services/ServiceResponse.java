package org.hivedb.services;

import java.util.Collection;

public interface ServiceResponse {
	Collection<ServiceContainer> getContainers();
	Collection<Object> getInstances();
}
