/**
 * 
 */
package org.hivedb.util;

public abstract class Predicate<T> {
	public abstract boolean f(final T item);
}