package org.hivedb.directory;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.Assigner;

public interface DirectoryFacadeProvider {
  DirectoryFacade getDirectoryFacade(HiveConfiguration hiveConfiguration, Assigner assigner);
}
