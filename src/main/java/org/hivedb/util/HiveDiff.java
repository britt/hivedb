package org.hivedb.util;

import java.util.Collection;
import java.util.Map;

import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

public class HiveDiff {

	Collection<Resource> missingResources;
	Map<Resource, Collection<SecondaryIndex>> missingSecondaryIndexes;
	public HiveDiff(
			Collection<Resource> missingResources,
			Map<Resource, Collection<SecondaryIndex>> missingSecondaryIndexes) {
		
		this.missingResources = missingResources;
		this.missingSecondaryIndexes = missingSecondaryIndexes;
	}
	public Collection<Resource> getMissingResources() {
		return missingResources;
	}
	public Map<Resource, Collection<SecondaryIndex>> getMissingSecondaryIndexes() {
		return missingSecondaryIndexes;
	}
	@Override
	public String toString() {
		return String.format(
							 "Missing resources: %s\n"+
							 "Missing secondary indexes: %s\n",
							 missingResources,
							 missingSecondaryIndexes);
	}

}
