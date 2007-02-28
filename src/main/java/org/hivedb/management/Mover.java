package org.hivedb.management;

import org.hivedb.management.quartz.Migration;

public interface Mover<T extends HivePersistable> {
	public MoveReport move(Migration migration, int rate);
	//This class should probably have a couple of checked exceptions for contigencies.
}
