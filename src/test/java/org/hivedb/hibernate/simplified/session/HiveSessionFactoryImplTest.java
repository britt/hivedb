package org.hivedb.hibernate.simplified.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Interceptor;
import org.hibernate.Session;
import org.hibernate.TransientObjectException;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.hibernate.RecordNodeOpenSessionEvent;
import org.hivedb.Assigner;
import org.hivedb.Node;
import org.hivedb.util.Lists;
import org.hivedb.util.database.Schemas;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

@HiveTest.Config("hive_default")
@RunWith(JMock.class)
public class HiveSessionFactoryImplTest extends HiveTest {
  private final static Log log = LogFactory.getLog(HiveSessionFactoryImplTest.class);
  HiveSessionFactoryImpl factory;
  private Mockery context;


  public void setup() {
    context = new JUnit4Mockery() {
      {
        //setImposteriser(ClassImposteriser.INSTANCE);
      }
    };

    if (factory == null)
      factory =
          (HiveSessionFactoryImpl)
              new SingletonHiveSessionFactoryBuilder(
                  getHive(),
                  Lists.newList(getMappedClasses()),
                  new SequentialShardAccessStrategy()).getSessionFactory();
    for (Node node : getHive().getNodes())
      Schemas.install(WeatherSchema.getInstance(), node.getUri());
  }

  @Test
  public void shouldOpenAllShardsSession() throws Exception {
    Session session = factory.openSession();
    assertNull(session.get(WeatherReport.class, new Integer(7)));
    session.close();
  }

  @Test
  public void shouldOpenAnAllShardsSessionWithTheSpecifiedInterceptor() throws Exception {
    final Interceptor mockInterceptor = context.mock(Interceptor.class);
    final WeatherReportImpl report = new WeatherReportImpl();
    context.checking(new Expectations() {
      {
        one(mockInterceptor).getEntityName(report);
        //will(returnValue());
      }
    });
    Session session = factory.openSession(mockInterceptor);
    try {
      session.getEntityName(report);
      fail("No exception throw");
    } catch (TransientObjectException e) {
      //this is just here because I don't want to mock every method that will be called.
    } finally {
      session.close();
    }
  }

  @Test
  public void shouldOpenASessionByPrimaryKey() throws Exception {
    Hive hive = getHive();
    String asia = "Asia";
    hive.directory().insertPrimaryIndexKey(asia);
    final WeatherReportImpl report = new WeatherReportImpl();
    report.setContinent(asia);
    Session session = factory.openSession(asia);
    try {
      Node node = getNodeForFirstId(hive, hive.directory().getNodeIdsOfPrimaryIndexKey(asia));
      assertCorrectNode(session, node);
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      session.close();
    }
  }

  @Test
  public void shouldOpenASessionByResourceId() throws Exception {
    Hive hive = getHive();
    String asia = "Asia";
    hive.directory().insertPrimaryIndexKey(asia);
    int id = 999;
    hive.directory().insertResourceId("WeatherReport", id, asia);
    final WeatherReportImpl report = new WeatherReportImpl();
    report.setContinent(asia);
    Session session = factory.openSession("WeatherReport", id);
    try {
      Node node = getNodeForFirstId(hive, hive.directory().getNodeIdsOfResourceId("WeatherReport", id));
      assertCorrectNode(session, node);
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      session.close();
    }
  }

  @Test
  public void shouldOpenASessionBySecondaryIndex() throws Exception {
    Hive hive = getHive();
    String asia = "Asia";
    int id = 999;
    hive.directory().insertPrimaryIndexKey(asia);
    hive.directory().insertResourceId("WeatherReport", id, asia);
    int code = 765;
    hive.directory().insertSecondaryIndexKey("WeatherReport", "RegionCode", code, id);

    final WeatherReportImpl report = new WeatherReportImpl();
    report.setContinent(asia);
    Session session = factory.openSession("WeatherReport", "RegionCode", code);
    try {
      Node node = getNodeForFirstId(hive, hive.directory().getNodeIdsOfSecondaryIndexKey("WeatherReport", "RegionCode", code));
      assertCorrectNode(session, node);
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      session.close();
    }
  }

  @Test
  public void shouldAddOpenSessionEvents() throws Exception {
    Hive hive = getHive();
    String asia = "Asia";
    hive.directory().insertPrimaryIndexKey(asia);
    final WeatherReportImpl report = new WeatherReportImpl();
    report.setContinent(asia);
    Session session = factory.openSession(asia);
    try {
      Node node = getNodeForFirstId(hive, hive.directory().getNodeIdsOfPrimaryIndexKey(asia));
      assertTrue("Opened a session to the wrong node", node.getUri().startsWith(RecordNodeOpenSessionEvent.getNode()));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception thrown: " + e.getMessage());
    } finally {
      session.close();
    }
  }

  @Test
  public void shouldAddOpenSessionEventsToAllShardsSession() throws Exception {
    Hive hive = getHive();
    String asia = "Asia";
    hive.directory().insertPrimaryIndexKey(asia);
    final WeatherReportImpl report = WeatherReportImpl.generate();
    report.setContinent(asia);
    Session session = factory.openSession();
    report.setReportId(88);
    try {
      session.save(report);
      Node node = getNodeForFirstId(hive, hive.directory().getNodeIdsOfPrimaryIndexKey(asia));
      assertTrue("Opened a session to the wrong node", node.getUri().startsWith(RecordNodeOpenSessionEvent.getNode()));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception thrown: " + e.getMessage());
    } finally {
      session.close();
    }
  }

  // TODO There is no guarantee that records get assigned to different nodes, causes intermittent failures
  @Test
  public void shouldThrowAnExceptionIfARecordIsStoredOnMoreThanOneNode() throws Exception {
    final Assigner assigner = context.mock(Assigner.class);
    final Hive hive = null; //= Hive.load(hiveConfiguration.getUri(getHive()), (HiveDataSourceProvider) getHive().getDataSourceProvider(), assigner);
    context.checking(new Expectations() {
      {
        exactly(2).of(assigner).chooseNode(with(any(Collection.class)), with(anything()));
        will(onConsecutiveCalls(returnValue(hive.getNode(1)), returnValue(hive.getNode(2))));
      }
    });

    String asia = "Asia";
    hive.directory().insertPrimaryIndexKey(asia);
    hive.directory().insertPrimaryIndexKey(asia);

    context.assertIsSatisfied(); //asserts that this is no longer probabalistic

    Session session = null;
    try {
      session = factory.openSession(asia);
      Collection<Integer> nodeIds = hive.directory().getNodeIdsOfPrimaryIndexKey(asia);
      assertEquals(2, nodeIds.size());
      Node node = getNodeForFirstId(hive, nodeIds);
      assertCorrectNode(session, node);
      fail("No exception thrown");
    } catch (IllegalStateException e) {

    } finally {
      if (session != null)
        session.close();
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private Node getNodeForFirstId(Hive hive, Collection<Integer> nodes) throws Exception {
    return hive.getNode(Atom.getFirst(nodes));
  }

  @SuppressWarnings("deprecation")
  public void assertCorrectNode(Session session, Node node) throws Exception {
    assertTrue("Opened a session to the wrong node", node.getUri().startsWith(session.connection().getMetaData().getURL()));
  }
}

