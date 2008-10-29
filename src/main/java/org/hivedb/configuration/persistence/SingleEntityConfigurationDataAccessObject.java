package org.hivedb.configuration.persistence;

public interface SingleEntityConfigurationDataAccessObject<T> extends ConfigurationDataAccessObject<T> {
  T get();
}
