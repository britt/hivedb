package org.hivedb.util.functional;

public interface DelayedTry<T> {
	T f() throws Exception;
}
