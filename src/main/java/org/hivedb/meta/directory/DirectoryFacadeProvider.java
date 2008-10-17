package org.hivedb.meta.directory;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.meta.Assigner;

public interface DirectoryFacadeProvider {
  DirectoryFacade getDirectoryFacade(HiveConfiguration hiveConfiguration, Assigner assigner);
}
