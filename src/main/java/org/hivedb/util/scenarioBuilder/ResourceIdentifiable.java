package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

public interface ResourceIdentifiable {
	
	/**
	 *  Construct a new ResourceIdentifiable form a prototype instance (an instance constructed with them no-arg constructor)
	 * @param primaryIndexIdentifiable
	 * @return
	 */
	ResourceIdentifiable construct(PrimaryIndexIdentifiable primaryIndexIdentifiable);
	Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables();
	PrimaryIndexIdentifiable getPrimaryIndexIdentifiable();
	String getResourceName();
}
