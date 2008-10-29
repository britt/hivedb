package org.hivedb.directory;

import org.hivedb.HiveRuntimeException;

public class DirectoryCorruptionException extends HiveRuntimeException {

	public DirectoryCorruptionException(String message) {
		super(message);
	}
	
	public DirectoryCorruptionException(String message, Exception inner) {
		super(message, inner);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
