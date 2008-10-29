package org.hivedb.directory;

public interface CacheKeyBuilder {
  String build(Object primaryIndexKey);

  String build(String resource, Object resourceId);

  String build(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId);

  String buildCounterKey(Object primaryIndexKey, String resource);

  String buildReferenceKey(Object primaryIndexKey, String resourceName, Long counter);
}
