package org.hivedb.configuration;

public interface EntityIndexConfigDelegator {
	EntityConfig getDelegateEntityConfig();
	Object stubEntityInstance(Object entityId, Object primaryIndexKey);
}
