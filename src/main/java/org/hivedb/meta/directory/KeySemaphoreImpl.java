package org.hivedb.meta.directory;

import org.hivedb.Lockable;
import org.hivedb.util.HiveUtils;

public class KeySemaphoreImpl implements KeySemaphore {
  private Status status;
  private int nodeId;
  private Object key;

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
}
