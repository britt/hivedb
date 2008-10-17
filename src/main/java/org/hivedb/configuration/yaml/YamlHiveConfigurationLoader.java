package org.hivedb.configuration.yaml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.functional.Factory;

public class YamlHiveConfigurationLoader implements Factory<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(YamlHiveConfigurationLoader.class);
  private String configFile;
  
  public HiveConfiguration newInstance() {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}

