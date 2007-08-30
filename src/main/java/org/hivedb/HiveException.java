/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 */
package org.hivedb;

@SuppressWarnings("serial")
public class HiveException extends Exception {
	public static final String CONNECTION_VALIDATION_ERROR = "Connection validator error";

	public HiveException(String message) {
		super(message);
	}
	public HiveException(String message, Exception inner) {
		super(message,inner);
	}
} // HiveException
