/**
 * 
 */
package org.hivedb.util;

import java.util.Map;

public interface PropertySetter<T> {
	void set(String property, Object value);
	Map getAsMap();
}