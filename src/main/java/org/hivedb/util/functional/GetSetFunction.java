package org.hivedb.util.functional;

public abstract class GetSetFunction {
	public abstract Object get();
	public abstract void set(Object value);
	public abstract Class getFieldClass();
}
