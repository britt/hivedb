package org.hivedb.util.validators;

import org.hivedb.HiveRuntimeException;
import org.hivedb.util.classgen.ReflectionTools;

public class NonNegativeValidator implements Validator {

	public boolean isValid(Object instance, String propertyName) {
		Object obj = ReflectionTools.invokeGetter(instance, propertyName);
		if (!(obj instanceof Number))
			throw new HiveRuntimeException("Expected an instance of type Number, but got " + obj.getClass().getSimpleName());
		Number num = (Number)obj;
		return obj.getClass().equals(long.class) || num.getClass().equals(Long.class) ? num.longValue() >= 0l : num.doubleValue() >= 0.0;
	}
	
	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException(String.format("Property %s of class %s is negative for instance %s", propertyName, instance.getClass().getSimpleName(), instance.toString()));
	}
}
