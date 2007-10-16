package org.hivedb.management.migration;

import java.util.Collection;
import java.util.Map.Entry;

public interface PartitionKeyMover<T> extends Mover<T> {
	public Collection<Entry<Mover, KeyLocator>> getDependentMovers(); 
}
