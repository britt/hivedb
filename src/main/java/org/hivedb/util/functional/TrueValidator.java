package org.hivedb.util.functional;

/***
 * The Identity validator, everything is valid.
 * @author bcrawford
 *
 */
public class TrueValidator implements Validator {
	public boolean isValid(Object obj) {return true;}
}
