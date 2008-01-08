package org.hivedb.services;

public interface ServiceContainer<T> {
	T getInstance();
	int getVersion();
}
