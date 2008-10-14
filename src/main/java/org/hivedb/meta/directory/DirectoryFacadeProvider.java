package org.hivedb.meta.directory;

import org.hivedb.Lockable;
import org.hivedb.meta.Assigner;

public interface DirectoryFacadeProvider {
  DirectoryFacade getDirectoryFacade(String hiveConfigurationUri, Assigner assigner, Lockable semaphore);
}
