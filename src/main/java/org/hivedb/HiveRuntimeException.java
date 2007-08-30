package org.hivedb;

/**
 * Used to capture unrecoverable checked exceptions, such as SQLException.
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
@SuppressWarnings("serial")
public class HiveRuntimeException extends RuntimeException {
	public HiveRuntimeException(String message) { 
		super(message);
	}
	public HiveRuntimeException(String message, Exception inner) {
		super(message,inner);
	}
}
