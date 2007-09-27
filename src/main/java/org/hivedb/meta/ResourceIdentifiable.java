package org.hivedb.meta;

import java.util.Collection;

public interface ResourceIdentifiable<F extends Object> {
	
	public String getIdPropertyName();
	public F getId(Object instance);

	Collection<? extends SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables();
	PrimaryIndexIdentifiable getPrimaryIndexIdentifiable();
	String getResourceName();
	
	boolean isPartitioningResource();
	Class getRepresentedInterface();
}
