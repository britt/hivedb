package org.hivedb;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.persistence.HiveDataSourceProvider;
import org.hivedb.util.Lists;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class TestConnectionManagerWithNodeOutOfService {
  private Mockery context;
  private DirectoryFacade directory;
  private HiveConfiguration hiveConfiguration;
  private HiveDataSourceProvider provider;
  private NodeImpl node;

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
    node = context.mock(NodeImpl.class);
  }


  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowAnExceptionWhenAccessingRecordsOnAnOutOfServiceNode() throws Exception {

    context.checking(new Expectations() {
      {
        one(hiveConfiguration).getNodes();
        will(returnValue(Lists.newList(node)));
      }
    });


    ConnectionManager cm = new ConnectionManager(directory, hiveConfiguration, provider);
    cm.daoSupport().getUnsafe("aNode");
  }

  @Test
  public void shouldContinueToFunctionWhenANodeIsMarkedOutOfService() throws Exception {
//    Hive hive = getHive();
//    Node outOfService = Atom.getFirst(hive.getNodes());
//    outOfService.setStatus(Lockable.Status.unavailable);
//    hive.updateNode(outOfService);
//    Node inService = Filter.grepSingle(new Predicate<Node>() {
//      public boolean f(Node item) {
//        return item.getStatus() != Lockable.Status.unavailable;
//      }
//    }, hive.getNodes());
//    hive.connection().daoSupport().getUnsafe(inService.getName());
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Test
  public void shouldContinueToFunctionWhenANodeGoesDownButIsNotMarkedOutOfService() throws Exception {
//    Hive hive = getHive();
//    Node down = new Node("down", "down", "", HiveDbDialect.H2);
//    hive.addNode(down);
//    Node notDown = Filter.grepSingle(new Predicate<Node>() {
//      public boolean f(Node item) {
//        return item.getName() != "down";
//      }
//    }, hive.getNodes());
//    hive.connection().daoSupport().getUnsafe(down.getName());
//    hive.connection().daoSupport().getUnsafe(notDown.getName());
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
