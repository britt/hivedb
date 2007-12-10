package org.hivedb.util.serialization;

public interface VersionUpgrader<T> {
	boolean isOldVersion(Integer serializedVersion);
	boolean isImplicitModernization(Integer fromVersion, Integer toVersion);
	Modernizer getModernizer(Integer fromVersion, Integer toVersion);
}
