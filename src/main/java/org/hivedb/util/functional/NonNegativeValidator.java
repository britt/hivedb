package org.hivedb.util.functional;

public class NonNegativeValidator<N extends Number> implements Validator<N> {

	public boolean isValid(N obj) {
		return obj.getClass().equals(long.class) || obj.getClass().equals(Long.class) ? obj.longValue() >= 0l : obj.doubleValue() >= 0.0;
	}

}
