package org.hivedb.meta.directory;

public interface ResourceKeySemaphore extends KeySemaphore {
  Object getPrimaryIndexKey();
}
