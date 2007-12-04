package org.hivedb.services;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

public class ServiceResponseImpl implements ServiceResponse, Serializable {
	private static final long serialVersionUID = 1L;
	private Collection<ServiceContainer> containers = Lists.newArrayList();
	
	public ServiceResponseImpl() {}

	public ServiceResponseImpl(final EntityConfig config, Collection<Object> instances) {
		this.containers = Transform.map(new Unary<Object, ServiceContainer>(){
			public ServiceContainer f(Object item) {
				return new ServiceContainerImpl(item, config.getVersion(item));
			}}, instances);
	}
	
	public ServiceResponseImpl(final EntityConfig config, Object... instances) {
		this(config, Arrays.asList(instances));
	}
	
	public ServiceResponseImpl(Collection<ServiceContainer> containers) {
		this.containers = containers;
	}

	public Collection<ServiceContainer> getContainers() {
		return containers;
	}

	public void setContainers(Collection<ServiceContainer> containers) {
		this.containers = containers;
	}

	public Collection<Object> getInstances() {
		return Transform.map(new Unary<ServiceContainer, Object>(){
			public Object f(ServiceContainer item) {
				return item.getInstance();
			}}, containers);
	}
}
