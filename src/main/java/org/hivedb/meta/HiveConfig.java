package org.hivedb.meta;

import java.util.Collection;

import org.hivedb.Hive;

/**
 *  An interface representing the configuration of a class that is represented as a resource in the hive.
 * @author andylikuski
 *
 */
public interface HiveConfig {
	
	EntityConfig<Object> getEntityConfig();
	Collection<Node> getDataNodes();
	Hive getHive();
}
