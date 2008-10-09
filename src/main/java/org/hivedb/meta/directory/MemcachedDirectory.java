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
 * There are a series of indirect links in the map. They can be confusing to follow. Its really hard to come up with meaningful
 * words that are not ambiguous to describe the keys, so you'll see P, PRC, PR, and R used in the code.
 * <p/>
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
    String P = keyBuilder.build(primaryIndexKey);
    return client.keyExists(P);
  }

  public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
    Preconditions.isNotNull(primaryIndexKey);
    String P = keyBuilder.build(primaryIndexKey);
    final KeySemaphore semaphore = (KeySemaphoreImpl) client.get(P);
    if (semaphore != null) {
      return Lists.newList(semaphore);
    } else {
      return new ArrayList<KeySemaphore>(0);
    }
  }

  public void deletePrimaryIndexKey(Object primaryIndexKey) {
    String P = keyBuilder.build(primaryIndexKey);
    client.delete(P);

    String counterCacheKey = null;

    ResourceCacheEntry entry = null;
    String PRC = null;
    for (Resource resource : partitionDimension.getResources()) {
      counterCacheKey = keyBuilder.buildCounterKey(primaryIndexKey, resource.getName());
      long resourceCount = client.getCounter(counterCacheKey);
      for (long i = 0; i < resourceCount; i++) {
        PRC = keyBuilder.buildReferenceKey(primaryIndexKey, resource.getName(), i);
        entry = (ResourceCacheEntry) client.get(PRC);
        try {
          client.delete(entry.getR());
        } catch (Exception e) {
          logOrphan(entry.getR());
        }
        try {
          client.delete(PRC);
        } catch (Exception e) {
          logOrphan(PRC);
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
    String R = keyBuilder.build(resource.getName(), id);
    String PR = (String) client.get(R);

    ResourceCacheEntry resourceCacheEntry = (ResourceCacheEntry) client.get(PR);

    KeySemaphore keySemaphore = (KeySemaphoreImpl) client.get(resourceCacheEntry.getP());
    return Lists.newList(keySemaphore);
  }

  public void deleteResourceId(Resource resource, Object id) {
    String R = keyBuilder.build(resource.getName(), id);
    String PR = (String) client.get(R);

    client.delete(PR);
    try {
      client.delete(R);
    } catch (Exception e) {
      logOrphan(R);
    }
  }

  public boolean doesSecondaryIndexKeyExist(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void deleteSecondaryIndexKey(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public boolean doesResourceIdExist(Resource resource, Object resourceId) {
    ResourceCacheEntry entry = null;
    try {
      String R = keyBuilder.build(resource.getName(), resourceId);
      String PR = (String) client.get(R);
      entry = (ResourceCacheEntry) client.get(PR);
    } catch (Exception e) {
    }
    return entry != null && client.keyExists(entry.getP());
  }

  public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
    String P = keyBuilder.build(primaryIndexKey);
    if (client.keyExists(P)) {
      throw new IllegalStateException(String.format("Can't insert if the key already exists [key=%1$s]", P));
    }
    KeySemaphoreImpl semaphore = new KeySemaphoreImpl(primaryIndexKey, node.getId());
    client.set(P, semaphore);
    for (Resource resource : partitionDimension.getResources()) {
      String PRC = keyBuilder.buildCounterKey(primaryIndexKey, resource.getName());
      client.storeCounter(PRC, 0);
    }
  }

  public void insertResourceId(Resource resource, Object id, Object primaryIndexKey) {
    String PRC = keyBuilder.buildCounterKey(primaryIndexKey, resource.getName());
    long counter = client.addOrIncr(PRC);
    String R = keyBuilder.build(resource.getName(), id);
    String P = keyBuilder.build(primaryIndexKey);
    ResourceCacheEntry entry = new ResourceCacheEntry(P, R);
    String PR = keyBuilder.buildReferenceKey(primaryIndexKey, resource.getName(), counter);
    client.set(PR, entry);
    client.set(R, PR);
  }

  public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    throw new UnsupportedOperationException("Secondary indices are not supported by this Directory.");
  }

  public void updatePrimaryIndexKeyOfResourceId(Resource r, Object resourceId, Object newPrimaryIndexKey) {
    deleteResourceId(r, resourceId);
    insertResourceId(r, resourceId, newPrimaryIndexKey);
  }

  public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean readOnly) {
    String P = keyBuilder.build(primaryIndexKey);
    KeySemaphoreImpl semaphore = (KeySemaphoreImpl) client.get(P);
    if (readOnly) {
      semaphore.setStatus(Lockable.Status.readOnly);
    } else {
      semaphore.setStatus(Lockable.Status.writable);
    }
    client.set(P, semaphore);
  }

  private ResourceCacheEntry getResourceCacheEntry(Resource resource, Object resourceId) {
    String R = keyBuilder.build(resource.getName(), resourceId);
    String PR = (String) client.get(R);
    return (ResourceCacheEntry) client.get(PR);
  }

  public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object resourceId) {
    ResourceCacheEntry R = getResourceCacheEntry(resource, resourceId);
    KeySemaphore semaphore = (KeySemaphore) client.get(R.getP());
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

    public ResourceCacheEntry(String P, String R) {
      data = new String[]{P, R};
    }

    public String getP() {
      return data[0];
    }

    public String getR() {
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
