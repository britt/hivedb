package org.hivedb.meta.directory;

import org.hivedb.util.Preconditions;

public class DefaultCacheKeyBuilder implements CacheKeyBuilder {
  private static final char DELIMITER = '-';
  private static final String PRIMARY_PREFIX = "pri" + DELIMITER;
  private static final String RESOURCE_PREFIX = "res" + DELIMITER;
  private static final String SECONDARY_PREFIX = "sec" + DELIMITER;

  public String build(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    return new StringBuilder(PRIMARY_PREFIX)
        .append(primaryIndexKey.toString())
        .toString();
  }

  public String build(String resource, Object resourceId) {
    Preconditions.isNotNull(resource, resourceId);
    return new StringBuilder(RESOURCE_PREFIX)
        .append(resource)
        .append(DELIMITER)
        .append(resourceId.toString())
        .toString();
  }

  public String build(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    Preconditions.isNotNull(resource, resourceId, secondaryIndex, secondaryIndexKey);
    return new StringBuilder(SECONDARY_PREFIX)
        .append(resource)
        .append(DELIMITER)
        .append(resourceId.toString())
        .append(DELIMITER)
        .append(secondaryIndex)
        .append(DELIMITER)
        .append(secondaryIndexKey.toString())
        .toString();
  }

}
