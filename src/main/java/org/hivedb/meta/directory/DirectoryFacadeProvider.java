package org.hivedb.meta.directory;

import org.hivedb.Lockable;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.PartitionDimension;

public interface DirectoryFacadeProvider {
  DirectoryFacade getDirectoryFacade(String hiveConfigurationUri, Assigner assigner, Lockable semaphore, PartitionDimension partitionDimension);
}
