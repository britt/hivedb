/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

public interface Predicate<T> {
	public boolean f(final T item);
}