package org.hivedb.util.validators;

import org.hivedb.HiveRuntimeException;
import org.hivedb.util.classgen.ReflectionTools;

public class NonNullValidator implements Validator {
	
	public boolean isValid(Object instance, String propertyName) {
		Object obj = ReflectionTools.invokeGetter(instance, propertyName);
		return obj != null;
	}

	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException(String.format("Property %s of class %s is null for instance %s", propertyName, instance.getClass().getSimpleName(), instance.toString()));
	}
}
