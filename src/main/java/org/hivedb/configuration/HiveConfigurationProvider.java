package org.hivedb.configuration;

public interface HiveConfigurationProvider {
  HiveConfiguration getHiveConfiguration(String hiveUri);
}
