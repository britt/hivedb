/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta;


public interface Loader<T> {
	T loadSingle(String uri);
}
