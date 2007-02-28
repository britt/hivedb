package org.hivedb.management.quartz;

import java.util.Collection;
import java.util.SortedSet;

import org.hivedb.meta.Node;

public interface NodeBalancer {
	public SortedSet<Migration> suggestMoves(Collection<Node> nodes);
}
