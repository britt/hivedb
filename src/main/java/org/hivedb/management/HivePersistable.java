package org.hivedb.management;

import org.hivedb.meta.PartitionDimension;

public interface HivePersistable {
	public Object getId();
	public Object getPartitioningKey();
	//If I don't find a need for this it will be pruned.
	public PartitionDimension getPartitionDimension();
}
