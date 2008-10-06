package org.hivedb.meta.directory;

public interface CacheKeyBuilder {
  String build(Object primaryIndexKey);

  String build(String resource, Object resourceId);

  String build(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId);
}
