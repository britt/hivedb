package org.hivedb.util;

import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;

/**
 *  Persists PrimaryIndexIdentifiables and ResourceIdentifiables. This interface allows persistence directly via a data access object class or indirectly
 *  via a web service proxy class or something similar.
 * @author andylikuski
 *
 */
public interface Persister {
	Object persistPrimaryIndexKey(EntityHiveConfig entityHiveConfig, Class representedInterface, final Object primaryIndexKey);
	Object persistResourceInstance(EntityHiveConfig entityHiveConfig, Class representedInterface, Object resourceInstance);
	Object persistSecondaryIndexKey(EntityHiveConfig entityHiveConfig, Class representedInterface, EntityIndexConfig secondaryIndexIdentifiable, Object resourceInstance);
}
