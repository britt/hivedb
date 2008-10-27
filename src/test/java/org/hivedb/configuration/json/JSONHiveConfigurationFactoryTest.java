package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.hivedb.HiveRuntimeException;

public class JSONHiveConfigurationFactoryTest {
  private final static Log log = LogFactory.getLog(JSONHiveConfigurationFactoryTest.class);

  @Test
  public void shouldLoadAJSONConfigurationFromAFile() throws Exception {
    throw new UnsupportedOperationException("Not yet implemented");    
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfTheFileCannotBeRead() throws Exception {
    JSONHiveConfigurationFactory factory = new JSONHiveConfigurationFactory("non-existent_test_config.js");
    factory.newInstance();
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfTheJSONCannotBeParsed() throws Exception {
    JSONHiveConfigurationFactory factory = new JSONHiveConfigurationFactory("invalid_test_config.js");
    factory.newInstance();
  }
}

