package org.hivedb.meta.directory;

public interface DirectoryProvider {
  DbDirectory newInstance();
}
