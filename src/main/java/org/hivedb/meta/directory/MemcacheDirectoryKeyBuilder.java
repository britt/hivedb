package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.Preconditions;
import org.hivedb.util.Strings;

public class MemcacheDirectoryKeyBuilder implements CacheKeyBuilder {
  private final static Log log = LogFactory.getLog(MemcacheDirectoryKeyBuilder.class);
  private static final String DELIMITER = "-";
  private static final String COUNTER_SUFFIX = "counter";
  private CacheKeyBuilder keyBuilder;
  private static final String REFERENCE_PREFIX = "ref";

  public MemcacheDirectoryKeyBuilder(CacheKeyBuilder keyBuilder) {
    this.keyBuilder = keyBuilder;
  }

  public String build(Object primaryIndexKey) {
    return keyBuilder.build(primaryIndexKey);
  }

  public String build(String resource, Object resourceId) {
    return keyBuilder.build(resource, resourceId);
  }

  public String build(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    return keyBuilder.build(resource, secondaryIndex, secondaryIndexKey, resourceId);
  }

  public String buildCounterKey(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    return Strings.join(DELIMITER, keyBuilder.build(primaryIndexKey), COUNTER_SUFFIX);
  }

  public String buildReferenceKey(Object primaryIndexKey, String resourceName, Integer counter) {
    Preconditions.isNotNull(primaryIndexKey, resourceName, counter);
    return Strings.join(DELIMITER, REFERENCE_PREFIX, primaryIndexKey.toString(), resourceName, counter.toString());
  }

}

