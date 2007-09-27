package org.hivedb.util;

public interface InstanceSetFunction<T,F> {
	public abstract void set(T instance, F value);
}
