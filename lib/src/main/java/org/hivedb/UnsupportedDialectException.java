package org.hivedb;

/**
 * A runtime fault thrown when HiveDB encounters a SQL dialect it cannot use.
 * @author bcrawford
 *
 */
public class UnsupportedDialectException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public UnsupportedDialectException(String msg) {
		super(msg);
	}
}
