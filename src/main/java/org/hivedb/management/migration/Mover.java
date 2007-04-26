package org.hivedb.management.migration;



public interface Mover<T> {
	public MoveReport move(Migration migration) throws MigrationException;
	//This class should probably have a couple of checked exceptions for contigencies.
}
