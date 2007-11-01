package org.hivedb;

import java.util.Collection;

import org.hivedb.meta.AccessType;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

public interface JdbcDaoSupportCache {
	public Collection<SimpleJdbcDaoSupport> get(Object primaryIndexKey, AccessType intention) throws HiveReadOnlyException;
	public Collection<SimpleJdbcDaoSupport> get(String resource, String secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveReadOnlyException;
	public Collection<SimpleJdbcDaoSupport> get(String resource, Object resourceId, AccessType intention) throws HiveReadOnlyException;
	public SimpleJdbcDaoSupport getUnsafe(String nodeName);
	public Collection<SimpleJdbcDaoSupport> getAllUnsafe();
}
