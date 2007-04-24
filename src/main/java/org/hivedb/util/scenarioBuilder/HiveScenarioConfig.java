package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.PrimaryIndexIdentifiable;

/**
 *  An interface to allow generating a hive schema and filling it with test data.
 *  The interface allows listing of various classes to represent a PartitionDimension and/or Resource
 *  PartitionDimension classes must implement PrimaryIndexIdentifiable to allow the HiveScenario to mode a PartitionDimension after it
 *  Resource classes must implement ResourceIdentifiable which in turn references a collection of SecondaryIndexIdentifiable instances.
 *  A class may be both a PrimaryIndexIdentifiable and ResourceIdentifiable if it is the basis of the PartitionDimension but also
 *  has SecondaryIndexes, such as an index for its getName() values.
 * @author andylikuski
 *
 */
public interface HiveScenarioConfig {
	
	PrimaryIndexIdentifiable getPrimaryIndexIdentifiable();
	
	Hive getHive();
	// The URI of the database that stores the hive indexes. This will probably always be the same as the hive URI, and might go away
	String getHiveIndexesUri();
	// The nodes of representing the data storage databases.
	Collection<Node> getDataNodes();
}
