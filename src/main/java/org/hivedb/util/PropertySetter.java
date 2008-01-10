/**
 * 
 */
package org.hivedb.util;

import java.util.Map;

import org.apache.cxf.aegis.type.java5.IgnoreProperty;

public interface PropertySetter<T> {
	void set(String property, Object value);
	@IgnoreProperty
	Map getAsMap();
}