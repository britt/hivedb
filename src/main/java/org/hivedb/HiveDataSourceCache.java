package org.hivedb;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.SecondaryIndex;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

public interface HiveDataSourceCache {
	public JdbcDaoSupport get(PartitionDimension partitionDimension, Object primaryIndexKey, AccessType intention) throws HiveReadOnlyException;
	public JdbcDaoSupport get(SecondaryIndex secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveReadOnlyException;
}
