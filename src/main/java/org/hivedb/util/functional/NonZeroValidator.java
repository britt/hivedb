package org.hivedb.util.functional;

public class NonZeroValidator implements Validator<Object> {
	public boolean isValid(Object obj) {
		if (obj.getClass().equals(Long.class) || obj.getClass().equals(Long.class ))
			return ((Number)obj).longValue() > 0l;
		if (obj.getClass().equals(Integer.class) || obj.getClass().equals(int.class))
			return ((Number)obj).intValue() > 0;
		if (obj.getClass().equals(Short.class) || obj.getClass().equals(short.class))
			return ((Number)obj).shortValue() > 0;
		throw new RuntimeException(String.format("Cannot validate Number of class %s", obj.getClass().getSimpleName()));
	}
}
