package org.hivedb.meta.directory;

public interface CacheKeyBuilder {
  enum Mode {
    key, semaphore
  }

  ;

  String build(Mode mode, Object primaryIndexKey);

  String build(Mode mode, String resource, Object resourceId);

  String build(Mode mode, String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId);
}
