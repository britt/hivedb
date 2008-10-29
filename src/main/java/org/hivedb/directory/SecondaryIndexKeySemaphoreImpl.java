package org.hivedb.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecondaryIndexKeySemaphoreImpl implements SecondaryIndexKeySemaphore{
  private final static Log log = LogFactory.getLog(SecondaryIndexKeySemaphoreImpl.class);

  private ResourceKeySemaphore semaphore;
  private Object secondaryIndexKey;

  public SecondaryIndexKeySemaphoreImpl(ResourceKeySemaphore semaphore, Object secondaryIndexKey) {
    this.semaphore = semaphore;
    this.secondaryIndexKey = secondaryIndexKey;
  }

  public Object getResourceId() {
    return semaphore.getKey();
  }

  public Object getPrimaryIndexKey() {
    return semaphore.getPrimaryIndexKey();
  }

  public Object getKey() {
    return this.secondaryIndexKey;
  }

  public int getNodeId() {
    return semaphore.getNodeId();
  }

  public Status getStatus() {
    return semaphore.getStatus();
  }
}

