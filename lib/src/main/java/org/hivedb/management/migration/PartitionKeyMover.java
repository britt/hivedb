package org.hivedb.management.migration;

import java.util.List;
import org.hivedb.util.functional.Pair;

public interface PartitionKeyMover<T> extends Mover<T> {
	public List<Pair<Mover, KeyLocator>> getDependentMovers(); 
}
