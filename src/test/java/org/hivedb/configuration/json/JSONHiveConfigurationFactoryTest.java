package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.*;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.sql.Types;
import java.util.Collection;
import java.util.List;

public class JSONHiveConfigurationFactoryTest {
  private final static Log log = LogFactory.getLog(JSONHiveConfigurationFactoryTest.class);

  private List<Node> getNodes() {
    return Lists.newList(new Node(0,"aNode","nodeDb","localhost", HiveDbDialect.H2));
  }

  private PartitionDimensionImpl getPartitionDimension() {
    Collection<SecondaryIndex> indexes = Lists.newList(new SecondaryIndex[]{new SecondaryIndexImpl(1, "anIndex", Types.VARCHAR)});
    Collection<Resource> resources = Lists.newArrayList();
    resources.add(new ResourceImpl(1, "aResource",Types.INTEGER, false, indexes));
    return new PartitionDimensionImpl(0,"aDimension", Types.INTEGER,"jdbc://uri", resources);
  }

  private HiveSemaphore getSemaphore() {
    return new HiveSemaphoreImpl(Lockable.Status.writable, 1);
  }

  @Test
  public void shouldLoadAHiveConfigurationFromAJSONFile() throws Exception {
    JSONHiveConfigurationFactory factory = new JSONHiveConfigurationFactory("src/test/resources/example_hive_configuration.js");
    HiveConfiguration config = factory.newInstance();
    assertEquals(getSemaphore(), config.getSemaphore());
    assertEquals(getPartitionDimension(), config.getPartitionDimension());
    assertEquals(getNodes(), config.getNodes());
  }


  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfTheFileCannotBeRead() throws Exception {
    JSONHiveConfigurationFactory factory = new JSONHiveConfigurationFactory("non-existent_test_config.js");
    factory.newInstance();
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfTheJSONCannotBeParsed() throws Exception {
    JSONHiveConfigurationFactory factory = new JSONHiveConfigurationFactory("src/test/resources/invalid_test_config.js");
    factory.newInstance();
  }
}

