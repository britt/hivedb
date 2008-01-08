package org.hivedb.services;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class ServiceResponseImpl<T> implements ServiceResponse<T>, Serializable {
	private static final long serialVersionUID = 1L;
	private Collection<ServiceContainer<T>> containers = Lists.newArrayList();
	
	public ServiceResponseImpl() {}

	public ServiceResponseImpl(final EntityConfig config, Collection<T> instances) {
		this.containers = Transform.map(new Unary<T, ServiceContainer<T>>(){
			public ServiceContainer<T> f(T item) {
				return new ServiceContainerImpl<T>(item, config.getVersion(item));
			}}, instances);
	}
	
	public ServiceResponseImpl(final EntityConfig config, T... instances) {
		this(config, Arrays.asList(instances));
	}
	
	public ServiceResponseImpl(Collection<ServiceContainer<T>> containers) {
		this.containers = containers;
	}

	public Collection<ServiceContainer<T>> getContainers() {
		return containers;
	}

	public void setContainers(Collection<ServiceContainer<T>> containers) {
		this.containers = containers;
	}

	public Collection<T> getInstances() {
		return Transform.map(new Unary<ServiceContainer<T>, T>(){
			public T f(ServiceContainer<T> item) {
				return item.getInstance();
			}}, containers);
	}
}
