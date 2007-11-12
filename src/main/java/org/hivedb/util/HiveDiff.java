package org.hivedb.util;

import java.util.Collection;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.functional.DebugMap;

public class HiveDiff {

	Collection<PartitionDimension> missingPartitionDimensions;
	Map<Hive, Collection<Node>> missingNodesOfExistingPartitionDimension;
	Map<PartitionDimension, Collection<Resource>> missingResourcesOfExistingPartitionDimension;
	Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> missingSecondaryIndexesOfExistingResources;
	public HiveDiff(
			Collection<PartitionDimension> missingPartitionDimensions,
			Map<Hive, Collection<Node>> missingNodesOfExistingPartitionDimension,
			Map<PartitionDimension, Collection<Resource>> missingResourcesOfExistingPartitionDimension,
			Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> missingSecondaryIndexesOfExistingResources) {
		
		this.missingPartitionDimensions = missingPartitionDimensions;
		this.missingNodesOfExistingPartitionDimension = missingNodesOfExistingPartitionDimension;
		this.missingResourcesOfExistingPartitionDimension = missingResourcesOfExistingPartitionDimension;
		this.missingSecondaryIndexesOfExistingResources = missingSecondaryIndexesOfExistingResources;
	}
	public Map<Hive, Collection<Node>> getMissingNodesOfExistingPartitionDimension() {
		return missingNodesOfExistingPartitionDimension;
	}
	public Collection<PartitionDimension> getMissingPartitionDimensions() {
		return missingPartitionDimensions;
	}
	public Map<PartitionDimension, Collection<Resource>> getMissingResourcesOfExistingPartitionDimension() {
		return missingResourcesOfExistingPartitionDimension;
	}
	public Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> getMissingSecondaryIndexesOfExistingResources() {
		return missingSecondaryIndexesOfExistingResources;
	}
	@Override
	public String toString() {
		return String.format("Missing partition dimensions: %s\n"+
							 "Missing resources of existing partition dimensions: %s\n"+
							 "Missing nodes of existing resources: %s\n"+
							 "Missing secondary indexes of existing secondary indexes: %s\n",
							 missingPartitionDimensions,
							 new DebugMap<PartitionDimension, Collection<Resource>>(missingResourcesOfExistingPartitionDimension),
							 new DebugMap<Hive, Collection<Node>>(missingNodesOfExistingPartitionDimension),
							 new DebugMap<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>>(missingSecondaryIndexesOfExistingResources));
	}

}
