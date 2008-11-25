package org.hivedb;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.persistence.HiveDataSourceProvider;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.util.Random;

@RunWith(JMock.class)
public class TestConnectionManagerWithNodeOutOfService {
  private Mockery context;
  private DirectoryFacade directory;
  private HiveConfiguration hiveConfiguration;
  private HiveDataSourceProvider provider;
  private static final String NODE_URI = "jdbc:h2:mem:db";
  private static final String NODE_NAME = "aNode";

  @Before
  public void setUp() throws Exception {
    context = new JUnit4Mockery() {
      {
        //setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    directory = context.mock(DirectoryFacade.class);
    hiveConfiguration = context.mock(HiveConfiguration.class);
    provider = context.mock(HiveDataSourceProvider.class);
  }


  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowAnExceptionWhenAccessingAnOutOfServiceNode() throws Exception {
    mockDataSourcesInitialized(stubNode(NODE_NAME, Lockable.Status.unavailable));

    ConnectionManager cm = new ConnectionManager(directory, hiveConfiguration, provider);
    cm.daoSupport().getUnsafe(NODE_NAME);
  }

  @Test
  public void shouldContinueToFunctionWhenANodeIsMarkedOutOfService() throws Exception {
    mockDataSourcesInitialized(
      stubNode(NODE_NAME, Lockable.Status.unavailable),
      stubNode("aGoodNode", Lockable.Status.writable));

    ConnectionManager cm = new ConnectionManager(directory, hiveConfiguration, provider);
    assertNotNull(cm.daoSupport().getUnsafe("aGoodNode"));
  }

  private Node stubNode(String name, Lockable.Status status) {
    NodeImpl node = new NodeImpl(new Random().nextInt(), name, "db", "", HiveDbDialect.H2);
    node.setStatus(status);
    return node;
  }

  private Node mockNode(final String nodeName, final Lockable.Status status) {
    final Node node = context.mock(Node.class);
    context.checking(new Expectations() {
      {
        allowing(node).getName();
        will(returnValue(nodeName));
        allowing(node).getDialect();
        will(returnValue(HiveDbDialect.H2));
        allowing(node).getUri();
        will(returnValue(NODE_URI));
        allowing(node).getId();
        will(returnValue(1));
        allowing(node).getStatus();
        will(returnValue(status));
      }
    });
    return node;
  }

  private void mockDataSourcesInitialized(final Node... nodes) {
    context.checking(new Expectations() {
      {
        allowing(hiveConfiguration).getNodes();
        will(returnValue(Lists.newList(nodes)));
        DataSource dataSource = context.mock(DataSource.class);        
        for(Node node : nodes) {
          exactly(2).of(provider).getDataSource(node.getUri());
          will(returnValue(dataSource));
        }
      }
    });
  }
}
