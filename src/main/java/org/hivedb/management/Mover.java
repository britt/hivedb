package org.hivedb.management;

import org.hivedb.management.Migration;

public interface Mover<T> {
	public MoveReport move(Migration migration) throws MigrationException;
	//This class should probably have a couple of checked exceptions for contigencies.
}
