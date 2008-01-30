package org.hivedb.util.functional;

import org.hivedb.HiveRuntimeException;

/***
 * The Identity validator, everything is valid.
 * @author bcrawford
 *
 */
public class TrueValidator implements Validator {
	public boolean isValid(Object instance, String propertyName) {return true;}
	
	public void throwInvalid(Object instance, String propertyName) {
		throw new HiveRuntimeException("TrueValidator should never throw");
	}
}
