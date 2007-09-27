package org.hivedb.management;

public interface KeyAuthority<T extends Object> {
	public T nextAvailableKey();
}
