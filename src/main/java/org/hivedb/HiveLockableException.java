package org.hivedb;

@SuppressWarnings("serial")
public class HiveLockableException extends HiveException {
	public HiveLockableException(String message) {
		super(message);
	}
	public HiveLockableException(String message, Exception inner) {
		super(message,inner);
	}
}
