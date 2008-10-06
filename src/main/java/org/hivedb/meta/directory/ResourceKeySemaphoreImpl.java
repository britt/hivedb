package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResourceKeySemaphoreImpl implements ResourceKeySemaphore {
  private final static Log log = LogFactory.getLog(ResourceKeySemaphoreImpl.class);

  private Object primaryKey;
  private KeySemaphore keySemaphore;

  public ResourceKeySemaphoreImpl(KeySemaphore keySemaphore, Object primaryKey) {
    this.keySemaphore = keySemaphore;
    this.primaryKey = primaryKey;
  }

  public int getNodeId() {
    return keySemaphore.getNodeId();
  }

  public Status getStatus() {
    return keySemaphore.getStatus();
  }

  public Object getPrimaryIndexKey() {
    return primaryKey;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ResourceKeySemaphoreImpl)) return false;

    ResourceKeySemaphoreImpl that = (ResourceKeySemaphoreImpl) o;

    if (keySemaphore != null ? !keySemaphore.equals(that.keySemaphore) : that.keySemaphore != null) return false;
    if (primaryKey != null ? !primaryKey.equals(that.primaryKey) : that.primaryKey != null) return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (primaryKey != null ? primaryKey.hashCode() : 0);
    result = 31 * result + (keySemaphore != null ? keySemaphore.hashCode() : 0);
    return result;
  }
}

