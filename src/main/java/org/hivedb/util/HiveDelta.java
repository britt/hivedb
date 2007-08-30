package org.hivedb.util;

import java.util.Collection;
import java.util.Map;

import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

public class HiveDelta {

	Collection<PartitionDimension> missingPartitionDimensions;
	Map<PartitionDimension, Collection<Node>> missingNodesOfExistingPartitionDimension;
	Map<PartitionDimension, Collection<Resource>> missingResourcesOfExistingPartitionDimension;
	Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> missingSecondaryIndexesOfExistingResources;
	public HiveDelta(
			Collection<PartitionDimension> missingPartitionDimensions,
			Map<PartitionDimension, Collection<Node>> missingNodesOfExistingPartitionDimension,
			Map<PartitionDimension, Collection<Resource>> missingResourcesOfExistingPartitionDimension,
			Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> missingSecondaryIndexesOfExistingResources) {
		
		this.missingPartitionDimensions = missingPartitionDimensions;
		this.missingNodesOfExistingPartitionDimension = missingNodesOfExistingPartitionDimension;
		this.missingResourcesOfExistingPartitionDimension = missingResourcesOfExistingPartitionDimension;
		this.missingSecondaryIndexesOfExistingResources = missingSecondaryIndexesOfExistingResources;
	}
	public Map<PartitionDimension, Collection<Node>> getMissingNodesOfExistingPartitionDimension() {
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

}
