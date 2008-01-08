package org.hivedb.services;

import java.io.Serializable;

public class ServiceContainerImpl<T> implements ServiceContainer<T>, Serializable {
	private static final long serialVersionUID = 1L;
	private T instance;
	private int version;

	public ServiceContainerImpl() {}
	
	public ServiceContainerImpl(T instance, int version) {
		this.instance = instance;
		this.version = version;
	}

	public T getInstance() {
		return instance;
	}

	public void setInstance(T instance) {
		this.instance = instance;
	}

	public int getVersion() {
		return version;
	}
	
	public void setVersion(int version) {
		this.version = version;
	}

}
