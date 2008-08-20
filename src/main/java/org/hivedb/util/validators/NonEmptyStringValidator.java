package org.hivedb.util.validators;

import org.hivedb.HiveRuntimeException;
import org.hivedb.util.classgen.ReflectionTools;

public class NonEmptyStringValidator implements Validator  {
	public boolean isValid(Object instance, String propertyName) {
		Object obj = ReflectionTools.invokeGetter(instance, propertyName);
		if (!(obj instanceof String))
			throw new HiveRuntimeException("Expected an instance of type String, but got " + obj.getClass().getSimpleName());
		String s = (String)obj;
		return s != null && s.length() > 0;
	}
	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException(String.format("Property %s of class %s is an empty string for instance %s", propertyName, instance.getClass().getSimpleName(), instance.toString()));
	}
}
