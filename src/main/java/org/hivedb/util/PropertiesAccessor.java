package org.hivedb.util;

public interface PropertiesAccessor {
	Object get(final String propertyName, final Object instance);
	void set(final String propertyName, final Object instance, final Object value);
	Class getFieldClass(final String propertyName);
}
