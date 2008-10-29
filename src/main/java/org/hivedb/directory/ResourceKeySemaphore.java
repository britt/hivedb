package org.hivedb.directory;

public interface ResourceKeySemaphore extends KeySemaphore {
  Object getPrimaryIndexKey();
}
