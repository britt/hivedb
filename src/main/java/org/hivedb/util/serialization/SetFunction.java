package org.hivedb.util.serialization;

public interface SetFunction<T> {
	public abstract void set(T value);
	public abstract Class getFieldClass();
}
