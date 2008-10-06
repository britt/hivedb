package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecondaryIndexKeySemaphoreImpl implements SecondaryIndexKeySemaphore{
  private final static Log log = LogFactory.getLog(SecondaryIndexKeySemaphoreImpl.class);

  private ResourceKeySemaphore semaphore;
  private Object resourceId;

  public SecondaryIndexKeySemaphoreImpl(ResourceKeySemaphore semaphore, Object resourceId) {
    this.resourceId = resourceId;
    this.semaphore = semaphore;
  }

  public Object getResourceId() {
    return resourceId;
  }

  public void setResourceId(Object resourceId) {
    this.resourceId = resourceId;
  }

  public Object getPrimaryIndexKey() {
    return semaphore.getPrimaryIndexKey();
  }

  public int getNodeId() {
    return semaphore.getNodeId();
  }

  public Status getStatus() {
    return semaphore.getStatus();
  }
}

