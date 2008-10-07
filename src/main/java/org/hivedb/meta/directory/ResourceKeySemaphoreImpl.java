package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResourceKeySemaphoreImpl implements ResourceKeySemaphore {
  private final static Log log = LogFactory.getLog(ResourceKeySemaphoreImpl.class);

  private Object resourceId;
  private KeySemaphore keySemaphore;

  public ResourceKeySemaphoreImpl(KeySemaphore keySemaphore, Object resourceId) {
    this.keySemaphore = keySemaphore;
    this.resourceId = resourceId;
  }
  
  public Object getKey() {
    return resourceId;
  }

  public int getNodeId() {
    return keySemaphore.getNodeId();
  }

  public Status getStatus() {
    return keySemaphore.getStatus();
  }

  public Object getPrimaryIndexKey() {
    return keySemaphore.getKey();
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ResourceKeySemaphoreImpl)) return false;

    ResourceKeySemaphoreImpl that = (ResourceKeySemaphoreImpl) o;

    if (keySemaphore != null ? !keySemaphore.equals(that.keySemaphore) : that.keySemaphore != null) return false;
    if (resourceId != null ? !resourceId.equals(that.resourceId) : that.resourceId != null) return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (resourceId != null ? resourceId.hashCode() : 0);
    result = 31 * result + (keySemaphore != null ? keySemaphore.hashCode() : 0);
    return result;
  }
}

