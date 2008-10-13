package org.hivedb.meta.directory;

public interface DirectoryProvider {
  DbDirectory getDirectory(String uri);
}
