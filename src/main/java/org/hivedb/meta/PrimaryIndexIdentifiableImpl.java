package org.hivedb.meta;

import org.hivedb.util.ReflectionTools;

public class PrimaryIndexIdentifiableImpl implements PrimaryIndexIdentifiable {
	
	public PrimaryIndexIdentifiableImpl(
			String partitionDimensionName,
			String primaryKeyPropertyName) {
		this.partitionDimensionName = partitionDimensionName;
		this.primaryKeyPropertyName = primaryKeyPropertyName;
	}
	
	private String partitionDimensionName;
	public String getPartitionDimensionName() {
		return partitionDimensionName;
	}
	
	private String primaryKeyPropertyName;
	public String getPrimaryKeyPropertyName() {
		return primaryKeyPropertyName;
	}
	public Object getPrimaryIndexKey(Object resourceInstance) {
		return ReflectionTools.invokeGetter(resourceInstance, primaryKeyPropertyName);
	}
	
}