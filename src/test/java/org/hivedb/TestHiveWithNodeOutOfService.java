package org.hivedb;

import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import static org.junit.Assert.fail;
import org.junit.Test;

@HiveTest.Config("hive_default")
public class TestHiveWithNodeOutOfService extends HiveTest {

  @Test
  public void shouldThrowAnExceptionWhenAccessingRecordsOnAnOutOfServiceNode() throws Exception {
    Hive hive = getHive();
    Node outOfService = Atom.getFirst(hive.getNodes());
    outOfService.setStatus(Lockable.Status.unavailable);
    hive.updateNode(outOfService);
    try {
        hive.connection().daoSupport().getUnsafe(outOfService.getName());
        fail("No exception thrown");
    } catch (Exception e) {
        //pass
    }

  }

  @Test
  public void shouldLoadWithAnOutOfServiceNode() throws Exception {
    Hive hive = getHive();
    Node outOfService = Atom.getFirst(hive.getNodes());
    outOfService.setStatus(Lockable.Status.unavailable);
    hive.updateNode(outOfService);
//    Hive newHive = Hive.load(hive.getHiveConfiguration().getUri(), CachingDataSourceProvider.getInstance());
//    assertNotNull(newHive);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Test
  public void shouldContinueToFunctionWhenANodeIsMarkedOutOfService() throws Exception {
    Hive hive = getHive();
    Node outOfService = Atom.getFirst(hive.getNodes());
    outOfService.setStatus(Lockable.Status.unavailable);
    hive.updateNode(outOfService);
    Node inService = Filter.grepSingle(new Predicate<Node>(){
      public boolean f(Node item) {
        return item.getStatus() != Lockable.Status.unavailable;
      }
    }, hive.getNodes());
    hive.connection().daoSupport().getUnsafe(inService.getName());
  }

  @Test
  public void shouldContinueToFunctionWhenANodeGoesDownButIsNotMarkedOutOfService() throws Exception {
    Hive hive = getHive();
    Node down = new Node("down","down","", HiveDbDialect.H2);
    hive.addNode(down);
    Node notDown = Filter.grepSingle(new Predicate<Node>(){
      public boolean f(Node item) {
        return item.getName() != "down";
      }
    }, hive.getNodes());
    hive.connection().daoSupport().getUnsafe(down.getName());
    hive.connection().daoSupport().getUnsafe(notDown.getName());
  }
}
