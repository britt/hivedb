package org.hivedb.meta.directory;

import org.hivedb.configuration.HiveConfiguration;

public interface DirectoryFactory {
  Directory newInstance(HiveConfiguration hiveConfiguration);
}
