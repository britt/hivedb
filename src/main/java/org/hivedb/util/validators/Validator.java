package org.hivedb.util.validators;

public interface Validator {
	public boolean isValid(Object instance, String propertyName);
	public void throwInvalid(Object instance, String propertyName);
}
