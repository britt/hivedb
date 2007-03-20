package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

public interface ResourceIdentifiable {
	Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables();
	Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass();
	PrimaryIndexIdentifiable getPrimaryIndexInstanceReference();
	Class getIdClass();
	String getResourceName();
}
