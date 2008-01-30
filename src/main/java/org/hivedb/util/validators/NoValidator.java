package org.hivedb.util.validators;

import org.hivedb.HiveRuntimeException;

/***
 * The Identity validator, everything is valid.
 * @author bcrawford
 *
 */
public class NoValidator implements Validator {
	public boolean isValid(Object instance, String propertyName) {return true;}
	
	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException("TrueValidator should never throw");
	}
}
