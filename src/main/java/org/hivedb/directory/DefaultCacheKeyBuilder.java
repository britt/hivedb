package org.hivedb.directory;

import org.hivedb.util.Preconditions;
import org.hivedb.util.Strings;

public class DefaultCacheKeyBuilder implements CacheKeyBuilder {
  private static final String DELIMITER = "-";
  private static final String PRIMARY_PREFIX = "pri";
  private static final String RESOURCE_PREFIX = "res";
  private static final String SECONDARY_PREFIX = "sec";
  private static final String COUNTER_SUFFIX = "counter";
  private static final String REFERENCE_PREFIX = "ref";


  public String build(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    return Strings.join(DELIMITER, PRIMARY_PREFIX, primaryIndexKey.toString());
  }

  public String build(String resource, Object resourceId) {
    Preconditions.isNotNull(resource, resourceId);
    return Strings.join(DELIMITER, RESOURCE_PREFIX, resource, resourceId.toString());
  }

  public String build(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    Preconditions.isNotNull(resource, resourceId, secondaryIndex, secondaryIndexKey);
    return Strings.join(DELIMITER, SECONDARY_PREFIX, resource, resourceId.toString(), secondaryIndex, secondaryIndexKey.toString());
  }

  public String buildCounterKey(Object primaryIndexKey, String resource) {
    Preconditions.isNotNull(primaryIndexKey);
    return Strings.join(DELIMITER, build(primaryIndexKey), resource, COUNTER_SUFFIX);
  }

  public String buildReferenceKey(Object primaryIndexKey, String resourceName, Long counter) {
    Preconditions.isNotNull(primaryIndexKey, resourceName, counter);
    return Strings.join(DELIMITER, REFERENCE_PREFIX, primaryIndexKey.toString(), resourceName, String.valueOf(counter));
  }
}
