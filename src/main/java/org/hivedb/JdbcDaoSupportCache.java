package org.hivedb;

import org.hivedb.meta.AccessType;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

import java.util.Collection;

public interface JdbcDaoSupportCache {
	public Collection<SimpleJdbcDaoSupport> get(Object primaryIndexKey, AccessType intention) throws HiveLockableException;
	public Collection<SimpleJdbcDaoSupport> get(String resource, String secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveLockableException;
	public Collection<SimpleJdbcDaoSupport> get(String resource, Object resourceId, AccessType intention) throws HiveLockableException;
	public SimpleJdbcDaoSupport getUnsafe(String nodeName);
	public Collection<SimpleJdbcDaoSupport> getAllUnsafe();
}
