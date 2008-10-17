package org.hivedb.configuration.yaml;

import org.hivedb.util.functional.Maps;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(JMock.class)
public class YamlHiveConfigurationLoaderTest {
  private Mockery mockery;
  
  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @Test
  public void shouldBuildAHiveConfiguration() throws Exception {}

  @Test
  public void shouldLoadHiveConfigurationFromAYamlFile() throws Exception {}

  private Map<String, Map<String, ?>> fakeYamlConfig() {
    Map<String, Map<String, ?>> yaml = Maps.newHashMap();
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
