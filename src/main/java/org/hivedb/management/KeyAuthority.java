package org.hivedb.management;

public interface KeyAuthority<T extends Number> {
	public T nextAvailableKey();
}
