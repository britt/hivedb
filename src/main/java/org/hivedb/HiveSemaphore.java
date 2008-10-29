package org.hivedb;

import org.hivedb.Lockable;

public interface HiveSemaphore extends Lockable {
  void setRevision(int revision);

  void setStatus(Status status);

  int getRevision();

  void incrementRevision();
}
