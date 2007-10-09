package org.hivedb.util;

import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.EntityIndexConfig;

/**
 *  Persists PrimaryIndexIdentifiables and ResourceIdentifiables. This interface allows persistence directly via a data access object class or indirectly
 *  via a web service proxy class or something similar.
 * @author andylikuski
 *
 */
public interface Persister {
	Object persistPrimaryIndexKey(final HiveConfig hiveConfig, final Object primaryIndexKey);
	Object persistResourceInstance(HiveConfig hiveConfig, Object resourceInstance);
	Object persistSecondaryIndexKey(final HiveConfig hiveConfig, EntityIndexConfig secondaryIndexIdentifiable, Object resourceInstance);
}
