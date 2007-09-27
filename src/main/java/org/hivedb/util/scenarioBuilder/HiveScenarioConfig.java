package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.ResourceIdentifiable;

/**
 *  An interface representing the configuration of a class that is represented as a resource in the hive.
 * @author andylikuski
 *
 */
public interface HiveScenarioConfig {
	
	/**
	 *  A tree of prototype instances representing the classes stored in the hive. The primaryIndexIdentifiable represents the partition dimension class.
	 *  The PrimaryIndexIdentifiable possesses one or more ResourceIdentifiable instances representing the resource classes.
	 *  Each ResourceIdentifiable possesses or or more SecondaryIndexIdentifiable instances represnting secondary index classes. 
	 * @return The top of the tree, the PrimaryIndexIdentifiable
	 */
	ResourceIdentifiable<Object> getResourceIdentifiable();
	
	Collection<Node> getDataNodes();
	/**
	 *  Returns the location and current configuration of the hive. The hive's current configuration should match that of the PrimaryIndexIdentifiable 
	 *  and its subordinates. If the PrimaryIndexIdentifiable structure has been updated then the Hive should be synced to it using HiveSyncer.
	 * @return
	 */
	Hive getHive();
	
}
