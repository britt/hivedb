package org.hivedb;

import java.util.Collection;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.SecondaryIndex;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

public interface JdbcDaoSupportCache {
	public Collection<SimpleJdbcDaoSupport> get(Object primaryIndexKey, AccessType intention) throws HiveReadOnlyException;
	public Collection<SimpleJdbcDaoSupport> get(SecondaryIndex secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveReadOnlyException;
	public SimpleJdbcDaoSupport getUnsafe(Node node);
	public SimpleJdbcDaoSupport getUnsafe(String nodeName);
}
