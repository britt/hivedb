package org.hivedb.util;

public abstract class GetSetFunction {
	public abstract Object get();
	public abstract void set(Object value);
	public abstract Class getFieldClass();
}
