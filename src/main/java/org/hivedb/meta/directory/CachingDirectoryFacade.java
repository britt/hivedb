package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveLockableException;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.util.Preconditions;
import org.hivedb.util.cache.Cache;
import org.hivedb.util.functional.Delay;

import java.util.Collection;
import java.util.Map;

/**
 * The cache is one big hashmap, with two kinds of lookups:
 * <p/>
 * primaryIndexKey -> node
 * resource -> primaryIndexKey
 * secondary -> primaryIndexKey
 * <p/>
 * deletes are by removing the primaryIndexKey -> node entry
 */
public class CachingDirectoryFacade implements DirectoryFacade {
  private final static Log log = LogFactory.getLog(CachingDirectoryFacade.class);

  private DirectoryFacade delegate;
  private Cache<String, Object> cache;
  private CacheKeyBuilder cacheKeyBuilder;

  public CachingDirectoryFacade(DirectoryFacade delegate, Cache<String, Object> cache, CacheKeyBuilder cacheKeyBuilder) {
    Preconditions.isNotNull(delegate, cache, cacheKeyBuilder);
    this.delegate = delegate;
    this.cache = cache;
    this.cacheKeyBuilder = cacheKeyBuilder;
  }

  public boolean doesPrimaryIndexKeyExist(final Object primaryIndexKey) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, primaryIndexKey);

    return existsInCacheOrDelegate(cacheKey, new Delay<Boolean>() {
      public Boolean f() {
        return delegate.doesPrimaryIndexKeyExist(primaryIndexKey);
      }
    });
  }

  private boolean existsInCacheOrDelegate(String cacheKey, Delay<Boolean> delay) {
    boolean inCache = false;
    try {
      inCache = cache.exists(cacheKey);
    } catch (Exception e) {
      log.error(String.format("Unable to check existence in cache [key: %1$s]", cacheKey), e);
    }
    return inCache || delay.f();
  }

  public Collection<Integer> getNodeIdsOfPrimaryIndexKey(final Object primaryIndexKey) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, primaryIndexKey);

    return readAndCache(cacheKey, new Delay<Collection<Integer>>() {
      public Collection<Integer> f() {
        return delegate.getNodeIdsOfPrimaryIndexKey(primaryIndexKey);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private <T> T readAndCache(String cacheKey, Delay<T> delay) {
    T retVal = null;
    try {
      retVal = (T) cache.get(cacheKey);
    } catch (Exception e) {
      log.error(String.format("Unable to read from cache [key: %1$s]", cacheKey), e);
    }
    if (retVal == null) {
      retVal = delay.f();
      try {
        cache.put(cacheKey, retVal);
      } catch (Exception e) {
        log.error(String.format("Unable to write to cache [key: %1$s]", cacheKey), e);
      }
    }
    return retVal;
  }

  public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(final Object primaryIndexKey) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.semaphore, primaryIndexKey);

    return readAndCache(cacheKey, new Delay<Collection<KeySemaphore>>() {
      public Collection<KeySemaphore> f() {
        return delegate.getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey);
      }
    });
  }

  public Collection getResourceIdsOfPrimaryIndexKey(final String resource, final Object primaryIndexKey) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, resource, primaryIndexKey);

    return readAndCache(cacheKey, new Delay<Collection>() {
      public Collection f() {
        return delegate.getResourceIdsOfPrimaryIndexKey(resource, primaryIndexKey);
      }
    });
  }

  public boolean getReadOnlyOfPrimaryIndexKey(final Object primaryIndexKey) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, primaryIndexKey);

    return readAndCache(cacheKey, new Delay<Boolean>() {
      public Boolean f() {
        return delegate.getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
      }
    });
  }

  public Integer insertPrimaryIndexKey(Object primaryIndexKey) throws HiveLockableException {
    Integer cacheValue = delegate.insertPrimaryIndexKey(primaryIndexKey);
    String cacheKey = null;
    try {
      cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, primaryIndexKey);
      cache.put(cacheKey, cacheValue);
    } catch (Exception e) {
      log.error(String.format("Unable to write to cache [key: %1$s]", cacheKey), e);
    }

    return cacheValue;
  }

  public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) throws HiveLockableException {
    // pkey semaphore
    delegate.updatePrimaryIndexKeyReadOnly(primaryIndexKey, isReadOnly);
  }

  public void deletePrimaryIndexKey(Object primaryIndexKey) throws HiveLockableException {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, primaryIndexKey);
    removeFromCache(cacheKey);
    cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.semaphore, primaryIndexKey);
    removeFromCache(cacheKey);
    delegate.deletePrimaryIndexKey(primaryIndexKey);
  }

  private void removeFromCache(String cacheKey) {
    try {
      cache.remove(cacheKey);
    } catch (Exception e) {
      log.error(String.format("Unable to remove from cache [key: %1$s]", cacheKey), e);
    }
  }

  public boolean doesSecondaryIndexKeyExist(final String resource, final String secondaryIndex, final Object secondaryIndexKey, final Object resourceId) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, resource, secondaryIndex, secondaryIndexKey, resourceId);

    return existsInCacheOrDelegate(cacheKey, new Delay<Boolean>() {
      public Boolean f() {
        return delegate.doesSecondaryIndexKeyExist(resource, secondaryIndex, secondaryIndexKey, resourceId);
      }
    });
  }

  public Collection<Integer> getNodeIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey) {
    return delegate.getNodeIdsOfSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey);
  }

  public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey) {
    return delegate.getKeySemaphoresOfSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey);
  }

  public void insertSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveLockableException {
    // 2 secondary maps
    delegate.insertSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey, resourceId);
  }

  public void deleteSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveLockableException {
    delegate.deleteSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey, resourceId);
  }

  public boolean doesResourceIdExist(final String resource, final Object resourceId) {
    String cacheKey = cacheKeyBuilder.build(CacheKeyBuilder.Mode.key, resource, resourceId);

    return existsInCacheOrDelegate(cacheKey, new Delay<Boolean>() {
      public Boolean f() {
        return delegate.doesResourceIdExist(resource, resourceId);
      }
    });
  }

  public Collection<Integer> getNodeIdsOfResourceId(String resource, Object id) {
    return delegate.getNodeIdsOfResourceId(resource, id);
  }

  public Collection<KeySemaphore> getKeySemaphoresOfResourceId(String resource, Object resourceId) {
    return delegate.getKeySemaphoresOfResourceId(resource, resourceId);
  }

  public boolean getReadOnlyOfResourceId(String resource, Object id) {
    return delegate.getReadOnlyOfResourceId(resource, id);
  }

  public void insertResourceId(String resource, Object id, Object primaryIndexKey) throws HiveLockableException {
    delegate.insertResourceId(resource, id, primaryIndexKey);
  }

  public void updatePrimaryIndexKeyOfResourceId(String resource, Object resourceId, Object newPrimaryIndexKey) throws HiveLockableException {
    delegate.updatePrimaryIndexKeyOfResourceId(resource, resourceId, newPrimaryIndexKey);
  }

  public void deleteResourceId(String resource, Object id) throws HiveLockableException {
    delegate.deleteResourceId(resource, id);
  }

  public Collection getSecondaryIndexKeysWithResourceId(String resource, String secondaryIndex, Object id) {
    return delegate.getSecondaryIndexKeysWithResourceId(resource, secondaryIndex, id);
  }

  public Collection getPrimaryIndexKeysOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey) {
    return delegate.getPrimaryIndexKeysOfSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey);
  }

  public Collection getResourceIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey) {
    return delegate.getResourceIdsOfSecondaryIndexKey(resource, secondaryIndex, secondaryIndexKey);
  }

  public Object getPrimaryIndexKeyOfResourceId(String name, Object resourceId) {
    return delegate.getPrimaryIndexKeyOfResourceId(name, resourceId);
  }

  public void deleteAllSecondaryIndexKeysOfResourceId(String resource, Object id) throws HiveLockableException {
    delegate.deleteAllSecondaryIndexKeysOfResourceId(resource, id);
  }

  public void deleteSecondaryIndexKeys(String resource, Map<String, Collection<Object>> secondaryIndexValueMap, Object resourceId) throws HiveLockableException {
    delegate.deleteSecondaryIndexKeys(resource, secondaryIndexValueMap, resourceId);
  }

  public void insertSecondaryIndexKeys(String resource, Map<String, Collection<Object>> secondaryIndexValueMap, Object resourceId) throws HiveLockableException {
    delegate.insertSecondaryIndexKeys(resource, secondaryIndexValueMap, resourceId);
  }
}
