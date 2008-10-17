package org.hivedb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.directory.DirectoryFacade;
import org.hivedb.meta.directory.DirectoryWrapperFactory;
import org.hivedb.meta.persistence.HiveDataSourceProvider;
import org.hivedb.util.functional.Factory;

public class HiveFactory implements Factory<Hive>  {
  private final static Log log = LogFactory.getLog(HiveFactory.class);

  private Assigner assigner;
  private Factory<HiveConfiguration> configurationProvider;
  private HiveDataSourceProvider dataSourceProvider;
  private DirectoryWrapperFactory directoryWrapperFactory;

  public HiveFactory(Factory<HiveConfiguration> configurationProvider, DirectoryWrapperFactory directoryWrapperFactory, HiveDataSourceProvider dataSourceProvider, Assigner assigner) {
    this.assigner = assigner;
    this.configurationProvider = configurationProvider;
    this.dataSourceProvider = dataSourceProvider;
    this.directoryWrapperFactory = directoryWrapperFactory;
  }

  public HiveFactory(Factory<HiveConfiguration> configurationProvider, DirectoryWrapperFactory directoryWrapperFactory, HiveDataSourceProvider dataSourceProvider) {
    this(configurationProvider, directoryWrapperFactory, dataSourceProvider, new RandomAssigner());
  }

  public Hive newInstance() {
    HiveConfiguration hiveConfiguration = configurationProvider.newInstance();
    DirectoryFacade directory = directoryWrapperFactory.getDirectoryFacade(hiveConfiguration, assigner);
    ConnectionManager connectionManager = new ConnectionManager(directory, hiveConfiguration, dataSourceProvider);    
    return new Hive(hiveConfiguration, directory, connectionManager, dataSourceProvider.getDataSource(hiveConfiguration.getUri()));
  }
}

