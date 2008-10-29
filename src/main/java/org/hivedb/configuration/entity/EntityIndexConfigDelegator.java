package org.hivedb.configuration.entity;

public interface EntityIndexConfigDelegator {
	EntityConfig getDelegateEntityConfig();
	Object stubEntityInstance(Object entityId, Object primaryIndexKey);
}
