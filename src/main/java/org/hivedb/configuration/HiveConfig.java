package org.hivedb.configuration;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;

public interface HiveConfig {

	public abstract Collection<Node> getDataNodes();

	public abstract Hive getHive();

}