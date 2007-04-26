package org.hivedb.management.migration;


public class MigrationException extends Exception {
	public MigrationException() {}
	
	public MigrationException(Exception innerException) {super(innerException);}
	
	public MigrationException(String msg) {super(msg);}
	
	public MigrationException(String msg, Exception innerException) {super(msg, innerException);}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 44048207672206567L;

}
