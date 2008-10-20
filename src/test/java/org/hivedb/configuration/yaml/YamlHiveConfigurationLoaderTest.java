package org.hivedb.configuration.yaml;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.functional.Maps;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(JMock.class)
public class YamlHiveConfigurationLoaderTest {
  private Mockery mockery;
  private YamlHiveConfigurationLoader factory;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };

    factory = new YamlHiveConfigurationLoader("/tmp/hive.yml");
  }

  @Test
  public void shouldBuildAHiveConfiguration() throws Exception {
    assertNotNull(factory.newInstance());
  }

  @Test
  public void shouldReturnThePathToTheConfigurationFileAsAUri() throws Exception {
    assertEquals("file:///tmp/hive.yml", factory.getFileUri());
  }

  private HiveConfiguration fakeHiveConfig() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Test
  public void shouldNotReloadTheConfigurationIfTheFileHasNotBeenModifiedSinceLastLoad() throws Exception {}

  @Test
  public void shouldThrowIfTheConfigurationFileDoesNotExist() throws Exception {}

  private Map<String, Map<String, ?>> fakeYamlConfig() {
    Map<String, Map<String, ?>> yaml = Maps.newHashMap();
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
