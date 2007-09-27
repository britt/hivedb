package org.hivedb.util.functional;

public class NonEmptyStringValidator implements Validator<String>  {
	public boolean isValid(String string) {
		return string != null && string.length() > 0;
	}
}
