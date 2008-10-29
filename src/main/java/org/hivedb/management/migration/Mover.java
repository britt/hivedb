package org.hivedb.management.migration;

import org.hivedb.Node;

public interface Mover<T> {
	public void copy(T item, Node node); 
	public T get(Object id, Node node);
	public void delete(T item, Node node);
}
