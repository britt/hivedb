package org.hivedb.util;

import java.util.Collection;
import java.util.Map;

import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.ResourceImpl;

public class HiveDiff {

	Collection<ResourceImpl> missingResources;
	Map<ResourceImpl, Collection<SecondaryIndex>> missingSecondaryIndexes;
	public HiveDiff(
			Collection<ResourceImpl> missingResources,
			Map<ResourceImpl, Collection<SecondaryIndex>> missingSecondaryIndexes) {
		
		this.missingResources = missingResources;
		this.missingSecondaryIndexes = missingSecondaryIndexes;
	}
	public Collection<ResourceImpl> getMissingResources() {
		return missingResources;
	}
	public Map<ResourceImpl, Collection<SecondaryIndex>> getMissingSecondaryIndexes() {
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
