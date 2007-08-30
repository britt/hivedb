package org.hivedb;

@SuppressWarnings("serial")
public class HiveReadOnlyException extends HiveException {
	public HiveReadOnlyException(String message) {
		super(message);
	}
	public HiveReadOnlyException(String message, Exception inner) {
		super(message,inner);
	}
}
