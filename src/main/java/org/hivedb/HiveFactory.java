package org.hivedb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.directory.DirectoryFacade;
import org.hivedb.meta.directory.DirectoryFacadeProvider;
import org.hivedb.meta.directory.DirectoryWrapperFactory;
import org.hivedb.meta.persistence.HiveDataSourceProvider;
import org.hivedb.util.functional.Factory;

public class HiveFactory implements Factory<Hive>  {
  private final static Log log = LogFactory.getLog(HiveFactory.class);

  private Assigner assigner;
  private Factory<HiveConfiguration> configurationFactory;
  private HiveDataSourceProvider dataSourceProvider;
  private DirectoryFacadeProvider directoryWrapperFactory;

  public HiveFactory(Factory<HiveConfiguration> configurationFactory, DirectoryFacadeProvider directoryWrapperFactory, HiveDataSourceProvider dataSourceProvider, Assigner assigner) {
    this.assigner = assigner;
    this.configurationFactory = configurationFactory;
    this.dataSourceProvider = dataSourceProvider;
    this.directoryWrapperFactory = directoryWrapperFactory;
  }

  public HiveFactory(Factory<HiveConfiguration> configurationProvider, DirectoryWrapperFactory directoryWrapperFactory, HiveDataSourceProvider dataSourceProvider) {
    this(configurationProvider, directoryWrapperFactory, dataSourceProvider, new RandomAssigner());
  }

  public Hive newInstance() {
    HiveConfiguration hiveConfiguration = configurationFactory.newInstance();
    DirectoryFacade directory = directoryWrapperFactory.getDirectoryFacade(hiveConfiguration, assigner);
    ConnectionManager connectionManager = new ConnectionManager(directory, hiveConfiguration, dataSourceProvider);    
    return new Hive(hiveConfiguration, directory, connectionManager, dataSourceProvider.getDataSource(hiveConfiguration.getUri()));
  }
}

