package org.hivedb.meta.directory;

import org.hivedb.Lockable;

public interface KeySemaphore extends Lockable {
  Object getKey();
  int getNodeId();
}
