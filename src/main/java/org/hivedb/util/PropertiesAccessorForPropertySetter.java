package org.hivedb.util;

public class PropertiesAccessorForPropertySetter extends PropertiesReflectionAccessor {	
	
	public PropertiesAccessorForPropertySetter(Class<?> representedInterface) {
		super(representedInterface);
	}

	public void set(final String propertyName, final PropertySetter instance, final Object value) {
		instance.set(propertyName, value);
	}
}
