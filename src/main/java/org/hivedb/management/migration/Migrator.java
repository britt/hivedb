package org.hivedb.management.migration;

import java.util.Collection;

public interface Migrator {

	/***
	 * Move a partition key and all its dependent records.
	 * @param key The primary index key to move
	 * @param destinationName the node to move it to.
	 * @param mover A mover to actually do the work
	 */
	@SuppressWarnings("unchecked")
	public abstract void migrate(Object key, Collection<String> destinationNames, PartitionKeyMover mover);

}