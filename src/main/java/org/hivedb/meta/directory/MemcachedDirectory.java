package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

import java.util.Collection;
import java.util.Map;

public class MemcachedDirectory implements Directory {
  private final static Log log = LogFactory.getLog(MemcachedDirectory.class);

  public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
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
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void deleteSecondaryIndexKey(SecondaryIndex index, Object secondaryIndexKey, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public boolean doesResourceIdExist(Resource resource, Object resourceId) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void insertPrimaryIndexKey(Node node, Object primaryIndexKey) {
    //todo: implement me
    throw new UnsupportedOperationException("Not yet implemented");
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
