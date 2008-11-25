package org.hivedb.hibernate.simplified.session;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.NodeImpl;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(JMock.class)
public class SingletonHiveSessionFactoryBuilderTest {
  SingletonHiveSessionFactoryBuilder builder;
  Hive hive;
  private Mockery mockery;


  @Before
  public void setup() {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };

    hive = mockery.mock(Hive.class);
    builder = new SingletonHiveSessionFactoryBuilder(hive, getMappedClasses(), new SequentialShardAccessStrategy());
  }

  @Test
  public void shouldBuildAProperlyConfiguredSessionFactory() throws Exception {
    mockFactoryCreated();

    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertNotNull(factory);
  }

  @Test
  public void shouldOnlyBuildASessionFactoryOnce() throws Exception {
    mockFactoryCreated();

    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    HiveSessionFactoryImpl anotherFactory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertTrue(factory == anotherFactory);
  }

  @Test
  public void shouldRebuildTheSessionFactoryWhenTheObservableUpdates() throws Exception {
    mockFactoryCreated();

    mockery.checking(new Expectations() {
      {
        one(hive).directory();
      }
    });


    HiveSessionFactoryImpl factory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    builder.update(null, null);
    HiveSessionFactoryImpl anotherFactory = (HiveSessionFactoryImpl) builder.getSessionFactory();
    assertTrue(factory != anotherFactory);
  }

  private List<Class<?>> getMappedClasses() {
    return Lists.newList(new Class<?>[]{WeatherReport.class, WeatherEvent.class});
  }

  private NodeImpl mockNode() {
    final NodeImpl node = mockery.mock(NodeImpl.class);
    mockery.checking(new Expectations() {
      {
        allowing(node).getName();
        will(returnValue("node"));
        allowing(node).getDialect();
        will(returnValue(HiveDbDialect.H2));
        allowing(node).getUri();
        will(returnValue("jdbc:h2:mem:db"));
        allowing(node).getId();
        will(returnValue(1));
      }
    });
    return node;

  }

  private void mockFactoryCreated() {
    mockery.checking(new Expectations() {
      {
        allowing(hive).getNodes();
        will(returnValue(Lists.newList(mockNode())));
        one(hive).directory();
      }
    });
  }
}
