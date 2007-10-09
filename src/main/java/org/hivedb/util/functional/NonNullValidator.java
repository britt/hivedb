package org.hivedb.util.functional;

public class NonNullValidator implements Validator<Object> {
	
	public boolean isValid(Object instance) {
		return instance != null;
	}
}
