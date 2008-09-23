package org.hivedb.hibernate.simplified.session;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.util.Lists;
import org.hivedb.util.database.test.HiveTest;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

@HiveTest.Config("hive_default")
public class SingletonHiveSessionFactoryBuilderTest extends HiveTest {
  SingletonHiveSessionFactoryBuilder builder;

  public void setup() {
    builder = new SingletonHiveSessionFactoryBuilder(getHive(), Lists.newList(getMappedClasses()), new SequentialShardAccessStrategy());
  }

  @Test
  public void shouldBuildAProperlyConfiguredSessionFactory() throws Exception {
    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertNotNull(factory);
  }

  @Test
  public void shouldOnlyBuildASessionFactoryOnce() throws Exception {
    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    HiveSessionFactoryImpl anotherFactory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertTrue(factory == anotherFactory);
  }

  @Test
  public void shouldRebuildTheSessionFactoryWhenTheObservableUpdates() throws Exception {
    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    builder.update(null,null);
    HiveSessionFactoryImpl anotherFactory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertTrue(factory != anotherFactory);
  }
}
