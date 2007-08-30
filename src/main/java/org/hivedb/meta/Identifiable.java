package org.hivedb.meta;

public interface Identifiable<T> {
	T getId();
	void setId(T field);
}
