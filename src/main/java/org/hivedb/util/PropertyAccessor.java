/**
 * 
 */
package org.hivedb.util;


public interface PropertyAccessor {
	void set(String property, Object value);
	Object get(String property);
}