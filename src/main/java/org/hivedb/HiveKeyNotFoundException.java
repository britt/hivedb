package org.hivedb;

public class HiveKeyNotFoundException extends HiveRuntimeException {
	/**
	 * Used on failed Directory look ups.
	 */
	private static final long serialVersionUID = 4084768603355365229L;
	private Object key;
	
	public Object getKey() {
		return key;
	}

	public HiveKeyNotFoundException(String message) {
		super(message);
	}
	
	public HiveKeyNotFoundException(String message, Object key) {
		super(message);
		this.key = key;
	}

	public HiveKeyNotFoundException(String message, Object key, Exception inner) {
		super(message, inner);
		this.key = key;
	}
}
