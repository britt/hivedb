package org.hivedb.util;

import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.SingularHiveConfig;

/**
 *  Persists PrimaryIndexIdentifiables and ResourceIdentifiables. This interface allows persistence directly via a data access object class or indirectly
 *  via a web service proxy class or something similar.
 * @author andylikuski
 *
 */
public interface Persister {
	Object persistPrimaryIndexKey(final SingularHiveConfig hiveConfig, final Object primaryIndexKey);
	Object persistResourceInstance(SingularHiveConfig hiveConfig, Object resourceInstance);
	Object persistSecondaryIndexKey(final SingularHiveConfig hiveConfig, EntityIndexConfig secondaryIndexIdentifiable, Object resourceInstance);
}
