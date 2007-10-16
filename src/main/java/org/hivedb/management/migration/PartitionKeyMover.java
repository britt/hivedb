package org.hivedb.management.migration;

import java.util.Collection;

import org.hivedb.util.functional.Pair;

public interface PartitionKeyMover<T> extends Mover<T> {
	public Collection<Pair<Mover, KeyLocator>> getDependentMovers(); 
}
