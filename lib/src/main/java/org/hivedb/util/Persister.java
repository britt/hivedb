package org.hivedb.util;

import org.hivedb.Hive;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndexIdentifiable;

public interface Persister {
	PrimaryIndexIdentifiable persistPrimaryIndexIdentifiable(final Hive hive, final PrimaryIndexIdentifiable newPrimaryIndexIdentifiable);
	ResourceIdentifiable persistResourceIdentifiableInstance(Hive hive, ResourceIdentifiable resourceIdentifiable);
	SecondaryIndexIdentifiable persistSecondaryIndexIdentifiableInstance(final Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifiable);
}
