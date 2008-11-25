/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb;

import java.util.Collection;

public interface Assigner {
	Node chooseNode(Collection<Node> nodes, Object value);
	Collection<Node> chooseNodes(Collection<Node> nodes, Object value);
}
