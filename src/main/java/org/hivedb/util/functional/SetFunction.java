package org.hivedb.util.functional;

public interface SetFunction<T> {
	public abstract void set(T value);
	public abstract Class getFieldClass();
}
