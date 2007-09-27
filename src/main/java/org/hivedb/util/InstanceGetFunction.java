package org.hivedb.util;

public interface InstanceGetFunction<T,F> {
	public abstract F get(T instance);
}
