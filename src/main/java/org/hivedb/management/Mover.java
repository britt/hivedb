package org.hivedb.management;

import org.hivedb.management.Migration;

public interface Mover<T extends HivePersistable> {
	public MoveReport move(Migration migration);
	//This class should probably have a couple of checked exceptions for contigencies.
}
