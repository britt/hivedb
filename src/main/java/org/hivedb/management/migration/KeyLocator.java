package org.hivedb.management.migration;

import java.util.Collection;

public interface KeyLocator<I,R> {
	public Collection<R> findAll(I parent);
}
