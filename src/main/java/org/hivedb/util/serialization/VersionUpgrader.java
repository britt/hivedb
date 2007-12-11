package org.hivedb.util.serialization;

import org.hivedb.serialization.Modernizer;

public interface VersionUpgrader<T> {
	boolean isOldVersion(Integer serializedVersion);
	boolean isImplicitModernization(Integer fromVersion, Integer toVersion);
	Modernizer getModernizer(Integer fromVersion, Integer toVersion);
}
