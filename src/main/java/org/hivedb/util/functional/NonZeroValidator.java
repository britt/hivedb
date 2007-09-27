package org.hivedb.util.functional;

public class NonZeroValidator<N extends Number> implements Validator<N> {
	public boolean isValid(N obj) {
		if (obj.getClass().equals(Long.class) || obj.getClass().equals(Long.class ))
			return obj.longValue() > 0l;
		if (obj.getClass().equals(Integer.class) || obj.getClass().equals(int.class))
			return obj.intValue() > 0;
		if (obj.getClass().equals(Short.class) || obj.getClass().equals(short.class))
			return obj.shortValue() > 0;
		throw new RuntimeException(String.format("Cannot validate Number of class %s", obj.getClass().getSimpleName()));
	}
}
