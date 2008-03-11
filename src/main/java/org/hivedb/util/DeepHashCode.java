package org.hivedb.util;

public interface DeepHashCode {
	/**
	 *  Returns a hash code composed up the entire object graph values.
	 * @param obj
	 * @return
	 */
	int deepHashCode(Object obj);
	int shallowHashCode(Object obj);
}
