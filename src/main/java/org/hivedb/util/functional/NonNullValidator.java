package org.hivedb.util.functional;

public class NonNullValidator<T> implements Validator<T> {
	
	public boolean isValid(T t) {
		return t != null;
	}
}
