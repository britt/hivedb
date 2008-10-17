package org.hivedb.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.functional.Factory;
import org.hivedb.util.SynchronizedSingletonProvider;

public class SynchronizedHiveConfigurationProvider implements Factory<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(SynchronizedHiveConfigurationProvider.class);

  private SynchronizedSingletonProvider<HiveConfiguration> configProvider;

  public SynchronizedHiveConfigurationProvider(Factory<HiveConfiguration> factory) {
    this.configProvider = new SynchronizedSingletonProvider<HiveConfiguration>(factory);
  }

  public HiveConfiguration newInstance() {
    return new DelegatingHiveConfiguration(configProvider.getSynchronizedInstance());
  }
}

