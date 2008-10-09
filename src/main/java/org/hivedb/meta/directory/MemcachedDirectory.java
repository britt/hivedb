package org.hivedb.meta.directory;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Lockable;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Lists;
import org.hivedb.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * This {@link Directory} implementation uses a map data structure to store the directory. It is backed by memcached.
 * <p/>
 * There are a series of indirect links in the map.
 * <p/>
 * KEY                                              VALUE
 * ===                                              =====
 * P = f(primaryIndexKey)                        => {@link org.hivedb.meta.directory.KeySemaphoreImpl} (contains node id and lock status)
 * <p/>
 * PRC = f(primaryIndexKey, resourceName)        => {@link Long} count of PR keys for this P
 * <p/>
 * PR = f(primaryIndexKey, resourceName, count)  => {@link org.hivedb.meta.directory.MemcachedDirectory.ResourceCacheEntry} (contains P and R refs)
 * <p/>
 * R = f(resourceName, resourceId)               => PR
 * <p/>
 * There are two ways to look at the resources belonging to a primary key:
 * <p/>
 * First, you can iterate over the collection by getting PRC then iterating from 0..PRC and then calculating key PR.
 * <p/>
 * Second, you can compute key R if you know the resourceId, then get PR.
 * <p/>
 * Because of the back-references to R in a PR, we can maintain the Map in a consistent state. No oprhaned entries are present under normal operation.
 */
public class MemcachedDirectory implements Directory {
  private final static Log log = LogFactory.getLog(MemcachedDirectory.class);
  private MemCachedClient client;
  private CacheKeyBuilder keyBuilder;
  private PartitionDimension partitionDimension;

  public MemcachedDirectory(String poolName, CacheKeyBuilder keyBuilder, PartitionDimension partitionDimension) {
    this(poolName, new MemCachedClient(poolName), keyBuilder, partitionDimension);
  }

  public MemcachedDirectory(String poolName, MemCachedClient client, CacheKeyBuilder keyBuilder, PartitionDimension partitionDimension) {
    if (!SockIOPool.getInstance(poolName).isInitialized()) {
      throw new IllegalStateException("Pool must be initialized.");
    }
    this.client = client;
    this.keyBuilder = keyBuilder;
    this.partitionDimension = partitionDimension;
  }


  public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    return client.keyExists(keyBuilder.build(primaryIndexKey));
  }

  public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    final KeySemaphore semaphore = (KeySemaphoreImpl) client.get(keyBuilder.build(primaryIndexKey));
    if (semaphore != null) {
      return Lists.newList(semaphore);
    } else {
      return new ArrayList<KeySemaphore>(0);
    }
  }

  public void deletePrimaryIndexKey(Object primaryIndexKey) {
    client.delete(keyBuilder.build(primaryIndexKey));

    String counterCacheKey = null;

    ResourceCacheEntry resourceEntry = null;
    String resourceCacheKey = null;
    for (Resource resource : partitionDimension.getResources()) {
      counterCacheKey = keyBuilder.buildCounterKey(primaryIndexKey, resource.getName());
      long resourceCount = client.getCounter(counterCacheKey);
      for (long i = 0; i < resourceCount; i++) {
        resourceCacheKey = keyBuilder.buildReferenceKey(primaryIndexKey, resource.getName(), i);
        resourceEntry = (ResourceCacheEntry) client.get(resourceCacheKey);
        try {
          client.delete(resourceEntry.getResourceBackreferenceCacheKey());
        } catch (Exception e) {
          logOrphan(resourceEntry.getResourceBackreferenceCacheKey());
        }
        try {
          client.delete(resourceCacheKey);
        } catch (Exception e) {
          logOrphan(resourceCacheKey);
        }
      }
    }
    try {
      client.delete(counterCacheKey);
    } catch (Exception e) {
      logOrphan(counterCacheKey);
    }
  }

  private void logOrphan(String key) {
    log.warn(String.format("left behind an orhpan at key [%1$s]", key));
  }

  public Collection<KeySemaphore> getKeySemaphoresOfResourceId(Resource resource, Object id) {
    String resourceCacheKey = keyBuilder.build(resource.getName(), id);
    String otherKey = (String) client.get(resourceCacheKey);

    ResourceCacheEntry resourceCacheEntry = (ResourceCacheEntry) client.get(otherKey);

    KeySemaphore keySemaphore = (KeySemaphoreImpl) client.get(resourceCacheEntry.getPrimaryIndexCacheKey());
    return Lists.newList(keySemaphore);
  }

  public void deleteResourceId(Resource resource, Object id) {
    String resourceCacheKey = keyBuilder.build(resource.getName(), id);
    String otherKey = (String) client.get(resourceCacheKey);

    client.delete(otherKey);
    try {
      client.delete(resourceCacheKey);
    } catch (Exception e) {
      logOrphan(resourceCacheKey);
    }
  }

  public boolean doesSecondaryIndexKeyExist(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void deleteSecondaryIndexKey(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public boolean doesResourceIdExist(Resource resource, Object resourceId) {
    ResourceCacheEntry resourceEntry = null;
    try {
      String resourceKey = keyBuilder.build(resource.getName(), resourceId);
      String otherKey = (String) client.get(resourceKey);
      resourceEntry = (ResourceCacheEntry) client.get(otherKey);
    } catch (Exception e) {
    }
    return resourceEntry != null && client.keyExists(resourceEntry.getPrimaryIndexCacheKey());
  }

  public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
    String p = keyBuilder.build(primaryIndexKey);
    if (client.keyExists(p)) {
      throw new IllegalStateException(String.format("Can't insert if the key already exists [key=%1$s]", p));
    }
    KeySemaphoreImpl semaphore = new KeySemaphoreImpl(primaryIndexKey, node.getId());
    client.set(p, semaphore);
    for (Resource resource : partitionDimension.getResources()) {
      client.storeCounter(keyBuilder.buildCounterKey(primaryIndexKey, resource.getName()), 0);
    }
  }

  public void insertResourceId(Resource resource, Object id, Object primaryIndexKey) {
    long counter = client.addOrIncr(keyBuilder.buildCounterKey(primaryIndexKey, resource.getName()));
    String resourceKey = keyBuilder.build(resource.getName(), id);
    ResourceCacheEntry entry = new ResourceCacheEntry(keyBuilder.build(primaryIndexKey), resourceKey);
    String referenceKey = keyBuilder.buildReferenceKey(primaryIndexKey, resource.getName(), counter);
    client.set(referenceKey, entry);

    client.set(resourceKey, referenceKey);
  }

  public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void updatePrimaryIndexKeyOfResourceId(Resource r, Object resourceId, Object newPrimaryIndexKey) {
    deleteResourceId(r, resourceId);
    insertResourceId(r, resourceId, newPrimaryIndexKey);
  }

  public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean readOnly) {
    String cacheKey = keyBuilder.build(primaryIndexKey);
    KeySemaphoreImpl semaphore = (KeySemaphoreImpl) client.get(cacheKey);
    if (readOnly) {
      semaphore.setStatus(Lockable.Status.readOnly);
    } else {
      semaphore.setStatus(Lockable.Status.writable);
    }
    client.set(cacheKey, semaphore);
  }

  private ResourceCacheEntry getResourceCacheEntry(Resource resource, Object resourceId) {
    String resourceCacheKey = keyBuilder.build(resource.getName(), resourceId);
    String otherKey = (String) client.get(resourceCacheKey);
    return (ResourceCacheEntry) client.get(otherKey);
  }

  public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object resourceId) {
    ResourceCacheEntry resourceCacheEntry = getResourceCacheEntry(resource, resourceId);
    KeySemaphore semaphore = (KeySemaphore) client.get(resourceCacheEntry.getPrimaryIndexCacheKey());
    return semaphore.getKey();
  }

  public void deleteSecondaryIndexKeys(Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void insertSecondaryIndexKeys(Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  @SuppressWarnings("unchecked")
  public Collection getSecondaryIndexKeysOfResourceId(SecondaryIndex secondaryIndex, Object id) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public static class ResourceCacheEntry implements Serializable {
    private String[] data;

    public ResourceCacheEntry(String primaryIndexCacheKey, String resourceBackreferenceCacheKey) {
      data = new String[]{primaryIndexCacheKey, resourceBackreferenceCacheKey};
    }

    public String getPrimaryIndexCacheKey() {
      return data[0];
    }

    public String getResourceBackreferenceCacheKey() {
      return data[1];
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ResourceCacheEntry that = (ResourceCacheEntry) o;

      if (!Arrays.equals(data, that.data)) return false;

      return true;
    }

    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }
}
