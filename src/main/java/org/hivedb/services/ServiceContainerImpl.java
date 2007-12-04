package org.hivedb.services;

import java.io.Serializable;

public class ServiceContainerImpl implements ServiceContainer, Serializable {
	private static final long serialVersionUID = 1L;
	private Object instance;
	private int version;

	public ServiceContainerImpl() {}
	
	public ServiceContainerImpl(Object instance, int version) {
		this.instance = instance;
		this.version = version;
	}

	public Object getInstance() {
		return instance;
	}

	public void setInstance(Object instance) {
		this.instance = instance;
	}

	public int getVersion() {
		return version;
	}
	
	public void setVersion(int version) {
		this.version = version;
	}

}
