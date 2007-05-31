package org.hivedb.management.migration;

public interface Migrator {

	@SuppressWarnings("unchecked")
	public abstract void migrate(Object key,
			String destinationName, PartitionKeyMover mover);

}