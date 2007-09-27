package org.hivedb.util;

import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;

/**
 *  Persists PrimaryIndexIdentifiables and ResourceIdentifiables. This interface allows persistence directly via a data access object class or indirectly
 *  via a web service proxy class or something similar.
 * @author andylikuski
 *
 */
public interface Persister {
	Object persistPrimaryIndexKey(final HiveScenarioConfig hiveScenarioConfig, final Object primaryIndexKey);
	Object persistResourceInstance(HiveScenarioConfig hiveScenarioConfig, Object resourceInstance);
	Object persistSecondaryIndexKey(final HiveScenarioConfig hiveScenarioConfig, SecondaryIndexIdentifiable secondaryIndexIdentifiable, Object resourceInstance);
}
