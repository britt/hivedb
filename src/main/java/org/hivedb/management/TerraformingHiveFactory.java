package org.hivedb.management;

import org.hivedb.Hive;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.meta.persistence.CachingDataSourceProvider;

import java.util.List;

public class TerraformingHiveFactory {
  public static Hive colonize(String uri, List<Class<?>> persistedClasses) {
    new HiveInstaller(uri).run();
    ConfigurationReader reader = new ConfigurationReader(persistedClasses);
    reader.install(uri);
    return Hive.load(uri, CachingDataSourceProvider.getInstance());
  }
}
