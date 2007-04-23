package org.hivedb.util;

import java.sql.SQLException;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.util.scenarioBuilder.PrimaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.ResourceIdentifiable;
import org.hivedb.util.scenarioBuilder.SecondaryIndexIdentifiable;

public interface Persister {
	PrimaryIndexIdentifiable persistPrimaryIndexIdentifiable(final Hive hive, final PrimaryIndexIdentifiable newPrimaryIndexIdentifiable) throws HiveException, SQLException;
	ResourceIdentifiable persistResourceIdentifiableInstance(Hive hive, ResourceIdentifiable resourceIdentifiable);
	SecondaryIndexIdentifiable persistSecondaryIndexIdentifiableInstance(final Hive hive, SecondaryIndexIdentifiable secondaryIndexIdentifiable) throws HiveException, SQLException;
}
