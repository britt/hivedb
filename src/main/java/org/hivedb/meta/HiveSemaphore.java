package org.hivedb.meta;

import org.hivedb.Lockable;

public interface HiveSemaphore extends Lockable {
  void setRevision(int revision);

  void setStatus(Status status);

  int getRevision();

  void incrementRevision();
}
