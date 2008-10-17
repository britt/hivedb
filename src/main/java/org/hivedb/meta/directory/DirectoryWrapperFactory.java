package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.meta.Assigner;
import org.hivedb.util.functional.Factory;

public class DirectoryWrapperFactory implements DirectoryFacadeProvider {
  private final static Log log = LogFactory.getLog(DirectoryWrapperFactory.class);
  private Factory<Directory> directoryProvider;

  public DirectoryWrapperFactory(Factory<Directory> directoryProvider) {
    this.directoryProvider = directoryProvider;
  }

  public DirectoryFacade getDirectoryFacade(HiveConfiguration hiveConfiguration, Assigner assigner) {
    return new DirectoryWrapper(
      directoryProvider.newInstance(),
      assigner,
      hiveConfiguration
    );
  }
}

