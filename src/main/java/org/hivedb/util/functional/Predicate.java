/**
 * 
 */
package org.hivedb.util.functional;

public interface Predicate<T> {
	public boolean f(final T item);
}