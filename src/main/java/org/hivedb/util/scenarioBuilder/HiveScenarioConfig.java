package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.Map;

import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;

public interface HiveScenarioConfig {
	int getInstanceCountPerPrimaryIndex();
	int getInstanceCountPerSecondaryIndex();
	// Classes to be used as primary indexes
	 Class[] getPrimaryClasses();
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	Class[] getResourceAndSecondaryIndexClasses();
	
	// The uris of the databases used for the index servers. For ease of testing, these
	// don't have to be unique
	Collection<String> getIndexUris(Hive hive);
	// The nodes of representing the data storage databases. These may be nonunique as well.
	Collection<Node> getNodes(Hive hive);
	Hive getHive();
	
	// Map relationships between the primary and resource classes
	Map<Class, Collection<Class>> getPrimaryToResourceMap();
	Map<Class, Class> getResourceToPrimaryMap();
	// This can probably be refactored out
	Map<String, Class> getResourceNameToClassMap();
}
