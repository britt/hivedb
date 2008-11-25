package org.hivedb.persistence;

import org.hivedb.Hive;
import org.hivedb.Node;
import org.hivedb.NodeImpl;
import org.hivedb.Lockable.Status;
import org.hivedb.configuration.persistence.NodeDao;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.List;

@Config("hive_default")
public class TestNodePersistence extends HiveTest {

  @Test
  public void testCreate() throws Exception {
    int count = getHive().getNodes().size();
    NodeDao dao = new NodeDao(getDataSource(getConnectString(getHiveDatabaseName())));
    assertEquals(count, dao.loadAll().size());

    NodeImpl full = createFullyPopulatedNode();
    NodeImpl minimal = createMinimalNode();

    dao.create(full);
    dao.create(minimal);

    List<Node> nodes = dao.loadAll();
    assertEquals(2 + count, nodes.size());

    Node fetchedFull = null;
    Node fetchedMinimal = null;

    for (Node n : nodes)
      if (n.getName().equals(full.getName()))
        fetchedFull = n;
      else if (n.getName().equals(minimal.getName()))
        fetchedMinimal = n;

    assertNotNull(fetchedFull);
    assertNotNull(fetchedMinimal);

    assertEquals(full, fetchedFull);
    assertEquals(minimal, fetchedMinimal);

    assertFalse(fetchedFull.getStatus().equals(Status.readOnly));
    assertFalse(fetchedMinimal.getStatus().equals(Status.readOnly));
  }

  @Test
  public void testUpdate() {
    int count = getHive().getNodes().size();
    NodeDao dao = new NodeDao(getDataSource(getConnectString(getHiveDatabaseName())));
    assertEquals(count, dao.loadAll().size());

    NodeImpl full = createFullyPopulatedNode();
    NodeImpl minimal = createMinimalNode();

    dao.create(full);
    dao.create(minimal);

    full.setDatabaseName("notBlahDatabase");

    minimal.setUsername("minimus");
    minimal.setPassword("maximus");

    dao.update(full);
    dao.update(minimal);

    List<Node> nodes = dao.loadAll();
    assertEquals(2 + count, nodes.size());

    Node fetchedFull = null;
    Node fetchedMinimal = null;

    for (Node n : nodes)
      if (n.getName().equals(full.getName()))
        fetchedFull = n;
      else if (n.getName().equals(minimal.getName()))
        fetchedMinimal = n;

    assertNotNull(fetchedFull);
    assertNotNull(fetchedMinimal);

    assertEquals(full, fetchedFull);
    assertEquals(minimal, fetchedMinimal);

    assertNotNull(fetchedMinimal.getUsername());
    assertNotNull(fetchedMinimal.getPassword());
  }

  @Test
  public void testDelete() {
    int count = getHive().getNodes().size();
    NodeDao dao = new NodeDao(getDataSource(getConnectString(getHiveDatabaseName())));
    assertEquals(count, dao.loadAll().size());

    NodeImpl full = createFullyPopulatedNode();
    NodeImpl minimal = createMinimalNode();

    dao.create(full);
    dao.create(minimal);

    List<Node> nodes = dao.loadAll();
    assertEquals(count + 2, nodes.size());
    for (Node n : nodes)
      dao.delete(n);

    assertEquals(0, dao.loadAll().size());
  }

  public NodeImpl createFullyPopulatedNode() {
    NodeImpl node = createMinimalNode();
    node.setName("full node");
    node.setStatus(Status.writable);
    node.setUsername("test");
    node.setPassword("test");
    node.setPort(3306);
    node.setCapacity(101);
    node.setOptions("&works=true");
    return node;
  }

  public NodeImpl createMinimalNode() {
    return new NodeImpl(
      Hive.NEW_OBJECT_ID,
      "minimal node",
      "blahbase",
      "localhost",
      HiveDbDialect.MySql
    );
  }
}
