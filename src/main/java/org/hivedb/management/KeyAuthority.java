package org.hivedb.management;

// TODO These KeyAuthority related classes need to be removed. They are really outside HiveDB's area of responsibility.
public interface KeyAuthority {
	public Object nextAvailableKey();
}
