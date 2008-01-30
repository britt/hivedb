package org.hivedb.util.functional;

import org.hivedb.HiveRuntimeException;
import org.hivedb.util.ReflectionTools;

public class NonZeroValidator implements Validator {
	public boolean isValid(Object instance, String propertyName) {
		Object obj = ReflectionTools.invokeGetter(instance, propertyName);
		if (obj.getClass().equals(Long.class) || obj.getClass().equals(Long.class ))
			return ((Number)obj).longValue() > 0l;
		if (obj.getClass().equals(Integer.class) || obj.getClass().equals(int.class))
			return ((Number)obj).intValue() > 0;
		if (obj.getClass().equals(Short.class) || obj.getClass().equals(short.class))
			return ((Number)obj).shortValue() > 0;
		throw new RuntimeException(String.format("Cannot validate Number of class %s", obj.getClass().getSimpleName()));
	}
	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException(String.format("Property %s of class %s is zero for instance %s", propertyName, instance.getClass().getSimpleName(), instance.toString()));
	}
}
