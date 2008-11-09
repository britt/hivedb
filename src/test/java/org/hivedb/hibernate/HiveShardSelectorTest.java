package org.hivedb.hibernate;

import org.hibernate.shards.ShardId;
import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.HiveLockableException;
import org.hivedb.configuration.entity.EntityConfig;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.directory.DirectoryWrapperFactory;
import org.hivedb.persistence.HiveDataSourceProvider;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Factory;
import org.hivedb.util.functional.Filter;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class HiveShardSelectorTest {
  private Mockery context;
  private Factory<Hive> hiveFactory;
  private Factory configurationProvider;
  private DirectoryWrapperFactory directoryWrapperFactory;
  private HiveDataSourceProvider dataSourceProvider;
  private EntityHiveConfig hiveConfig;
  private DirectoryFacade directory;
  private HiveShardSelector selector;

  @Before
  public void setup() throws Exception {
    context = new JUnit4Mockery() {
      {
        //setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    hiveConfig = context.mock(EntityHiveConfig.class);
    directory = context.mock(DirectoryFacade.class);
    selector = new HiveShardSelector(hiveConfig, directory);
  }

  @Test
  public void shouldSelectAShard() throws Exception {
    final Collection<Integer> nodeIds = Lists.newList(1, 2, 3);
    final Object entity = new Object();

    final EntityConfig entityConfig = mockEntityConfigFound(true);
    mockPartitionKeyExists(true, entity, entityConfig);
    mockPartitionKeyNodeLookup();

    ShardId id = selector.selectShardIdForNewObject(entity);
    assertNotNull(id);
    assertTrue(Filter.grepItemAgainstList(new Integer(id.getId()), nodeIds));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfGivenAnUnknownClass() throws Exception {
    final Collection<Integer> nodeIds = Lists.newList(1, 2, 3);
    final Object entity = new Object();

    final EntityConfig entityConfig = mockEntityConfigFound(false);
    ShardId id = selector.selectShardIdForNewObject(entity);
  }

  @Test
  public void shouldInsertThePrimaryKeyIfItDoesNotExist() throws Exception {
    final Collection<Integer> nodeIds = Lists.newList(1, 2, 3);
    final Object entity = new Object();

    final EntityConfig entityConfig = mockEntityConfigFound(true);
    mockPartitionKeyExists(false, entity, entityConfig);
    mockPartitionKeyInserted();
    mockPartitionKeyNodeLookup();

    ShardId id = selector.selectShardIdForNewObject(entity);
    assertNotNull(id);
    assertTrue(Filter.grepItemAgainstList(new Integer(id.getId()), nodeIds));
  }

  private void mockPartitionKeyInserted() throws HiveLockableException {
    context.checking(new Expectations() {
      {
        one(directory).insertPartitionKey("key");
      }
    });

  }

  private EntityConfig mockEntityConfigFound(final boolean isFound) {
    final EntityConfig returnValue = isFound ? context.mock(EntityConfig.class) : null;
    context.checking(new Expectations() {
      {
        one(hiveConfig).getEntityConfig(Object.class);
        will(returnValue(returnValue));
      }
    });
    return returnValue;
  }

  private void mockPartitionKeyNodeLookup() {
    context.checking(new Expectations() {
      {
        one(directory).getNodeIdsOfPartitionKey("key");
        will(returnValue(Lists.newList(1)));
      }
    });
  }

  private void mockPartitionKeyExists(final boolean exists, final Object entity, final EntityConfig entityConfig) {
    context.checking(new Expectations() {
      {
        one(entityConfig).getPartitionKey(entity);
        will(returnValue("key"));
        one(directory).doesPartitionKeyExist("key");
        will(returnValue(exists));
      }
    });
  }
//  @Test
//  public void testSelectNode() throws Exception {
//    ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
//    Hive hive = getHive();
//    HiveShardSelector selector = new HiveShardSelector(reader.getHiveConfiguration(), hive.directory());
//    WeatherReport report = WeatherReportImpl.generate();
//
//    ShardId id = selector.selectShardIdForNewObject(report);
//    Assert.assertNotNull(id);
//
//    Collection<Integer> nodeIds = Transform.map(new Unary<Node, Integer>() {
//      public Integer f(Node n) {
//        return n.getId();
//      }
//    }, hive.getNodes());
//
//    assertTrue(Filter.grepItemAgainstList(new Integer(id.getId()), nodeIds));
//  }
}
