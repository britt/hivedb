package org.hivedb.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.Assigner;

public class DirectoryWrapperFactory implements DirectoryFacadeProvider {
  private final static Log log = LogFactory.getLog(DirectoryWrapperFactory.class);
  private DirectoryFactory directoryProvider;

  public DirectoryWrapperFactory(DirectoryFactory directoryProvider) {
    this.directoryProvider = directoryProvider;
  }

  public DirectoryFacade getDirectoryFacade(HiveConfiguration hiveConfiguration, Assigner assigner) {
    return new DirectoryWrapper(
      directoryProvider.newInstance(hiveConfiguration),
      assigner,
      hiveConfiguration
    );
  }
}

