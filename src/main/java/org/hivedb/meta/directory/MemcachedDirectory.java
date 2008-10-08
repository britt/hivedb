package org.hivedb.meta.directory;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Lockable;
import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class MemcachedDirectory implements Directory {
  private final static Log log = LogFactory.getLog(MemcachedDirectory.class);
  private MemCachedClient client;
  private MemcacheDirectoryKeyBuilder keyBuilder;

  public MemcachedDirectory(String poolName, MemcacheDirectoryKeyBuilder keyBuilder) {
    this(poolName, new MemCachedClient(poolName), keyBuilder);
  }

  public MemcachedDirectory(String poolName, MemCachedClient client, MemcacheDirectoryKeyBuilder keyBuilder) {
    if (!SockIOPool.getInstance(poolName).isInitialized()) {
      throw new IllegalStateException("Pool must be initialized.");
    }
    this.client = client;
    this.keyBuilder = keyBuilder;
  }

  public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
    return client.keyExists(keyBuilder.build(primaryIndexKey));
  }

  public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
    final Collection<KeySemaphore> results = (Collection<KeySemaphore>) client.get(keyBuilder.build(primaryIndexKey));
    return results == null ? new ArrayList<KeySemaphore>(0) : results;
  }

  public void deletePrimaryIndexKey(Object primaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Collection<KeySemaphore> getKeySemaphoresOfResourceId(Resource resource, Object id) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void deleteResourceId(Resource resource, Object id) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public boolean doesSecondaryIndexKeyExist(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    String referenceKey = client.get(keyBuilder.build(index.getResource().getName(), index.getName(), secondaryIndexKey, resourceId)).toString();
    return referenceKey != null && client.keyExists(referenceKey);
  }

  public void deleteSecondaryIndexKey(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public boolean doesResourceIdExist(Resource resource, Object resourceId) {
    String referenceKey = client.get(keyBuilder.build(resource.getName(), resourceId)).toString();
    return referenceKey != null && client.keyExists(referenceKey);
  }

  public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
    // TODO: This only supports single assignment. Decide whether to refactor or support multiple assignment
    if(!doesPrimaryIndexKeyExist(primaryIndexKey)) {
      client.add(keyBuilder.build(primaryIndexKey), new KeySemaphoreImpl(primaryIndexKey, node.getId(), Lockable.Status.writable));
      client.addOrIncr(keyBuilder.buildCounterKey(primaryIndexKey));
    } else {
      throw new IllegalStateException("Primary index key already exists. Use update instead.");
    }
  }

  public void insertResourceId(Resource resource, Object id, Object primaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void updatePrimaryIndexKeyOfResourceId(Resource r, Object resourceId, Object newPrimaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean readOnly) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void deleteSecondaryIndexKeys(Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void insertSecondaryIndexKeys(Map<SecondaryIndex, Collection<Object>> secondaryIndexValueMap, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @SuppressWarnings("unchecked")
  public Collection getSecondaryIndexKeysOfResourceId(SecondaryIndex secondaryIndex, Object id) {
//todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
