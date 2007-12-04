package org.hivedb.services;

public interface ServiceContainer {
	Object getInstance();
	int getVersion();
}
