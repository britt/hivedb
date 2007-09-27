package org.hivedb.util.functional;

public interface Validator<T> {
	public boolean isValid(T obj);
}
