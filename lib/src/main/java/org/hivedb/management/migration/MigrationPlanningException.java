package org.hivedb.management.migration;


public class MigrationPlanningException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3126324251753631617L;

	public MigrationPlanningException(){
		super("Unable to compute a valid migration plan.");
	}
	
	public MigrationPlanningException(String message) {
		super(message);
	}
}
