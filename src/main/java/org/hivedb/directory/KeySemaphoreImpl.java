package org.hivedb.directory;

import org.hivedb.Lockable;
import org.hivedb.util.HiveUtils;

import java.io.Serializable;

public class KeySemaphoreImpl implements KeySemaphore, Serializable {
  private Status status;
  private int nodeId;
  private Object key;

  public KeySemaphoreImpl(Object key, int nodeId) {
    this(key, nodeId, Lockable.Status.writable);
  }

  public KeySemaphoreImpl(Object key, int nodeId, Lockable.Status status) {
    this.nodeId = nodeId;
    this.status = status;
    this.key = key;
  }

  public Object getKey() {
    return key;
  }

  public int getNodeId() {
    return nodeId;
  }

  public boolean equals(Object obj) {
    return obj.hashCode() == hashCode();
  }

  public int hashCode() {
    return HiveUtils.makeHashCode(new Object[]{
        nodeId, status
    });
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }
}
