package org.hivedb.management.migration;

import org.hivedb.Hive;
import org.hivedb.Node;
import org.hivedb.persistence.Schema;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.persistence.TableInfo;
import org.hivedb.directory.DbDirectory;
import org.hivedb.directory.DirectoryWrapper;
import org.hivedb.directory.NodeResolver;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

public class TestMigration extends HiveTest {

  public void setup() {
    for (String name : getDatabaseNames()) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(getDataSource(getConnectString(name)));

      try {
        dao.getJdbcTemplate().update("SET DB_CLOSE_DELAY 5");
      }
      catch (Exception e) {
      } // only protection against multiple calls (create a Schema class to avoid this)
    }
  }

  @Test
  public void testMigration() throws Exception {
    Hive hive = null;//= Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    String primaryKey = new String("Asia");
    Integer secondaryKey = new Integer(7);

    Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
    Node origin = nodes.getKey();
    Node destination = nodes.getValue();
    NodeResolver dir = new DbDirectory(hive.getHiveConfiguration(), CachingDataSourceProvider.getInstance().getDataSource(getConnectString(getHiveDatabaseName())));
    PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
    Mover<Integer> secMover = new SecondaryMover();

    //Do the actual migration
    Migrator m = new HiveMigrator(hive);
    assertNotNull(Filter.grepItemAgainstList(origin.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
    m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
    //Directory points to the destination node
    assertNotNull(Filter.grepItemAgainstList(destination.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
    //Records exist and are identical on the destination node
    assertEquals(primaryKey, pMover.get(primaryKey, destination));
    assertEquals(secondaryKey, secMover.get(secondaryKey, destination));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFailDuringCopy() throws Exception {
    Hive hive = null;// = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    String primaryKey = new String("Oceana");
    Integer secondaryKey = new Integer(7);

    Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
    Node origin = nodes.getKey();
    Node destination = nodes.getValue();
    NodeResolver dir = new DbDirectory(hive.getHiveConfiguration(), getDataSource(getConnectString(getHiveDatabaseName())));
    PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
    //This mover just craps out on copy
    Mover<Integer> failingMover = new Mover<Integer>() {
      public void copy(Integer item, Node node) {
        throw new RuntimeException("");
      }

      public void delete(Integer item, Node node) {
      }

      public Integer get(Object id, Node node) {
        return null;
      }
    };

    Migrator m = new HiveMigrator(hive);
    pMover.getDependentMovers().clear();
    pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
    try {
      m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
    } catch (Exception e) {
      //Quash
    }
    //Directory still points to the origin node
    assertNotNull(Filter.grepItemAgainstList(origin.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
    //Records are intact on the origin node
    assertEquals(primaryKey, pMover.get(primaryKey, origin));
    assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, origin));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFailDuringDelete() throws Exception {
    Hive hive = null;// = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    String primaryKey = new String("Asia");
    Integer secondaryKey = new Integer(7);

    Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
    Node origin = nodes.getKey();
    Node destination = nodes.getValue();

    NodeResolver dir = new DbDirectory(hive.getHiveConfiguration(), getDataSource(getConnectString(getHiveDatabaseName())));
    PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
//		This mover just craps out on delete
    Mover<Integer> failingMover = new Mover<Integer>() {
      public void copy(Integer item, Node node) {
        SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
        dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
        dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
      }

      public void delete(Integer item, Node node) {
        throw new RuntimeException("Ach!");
      }

      public Integer get(Object id, Node node) {
        SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
        dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
        return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
      }
    };

    Migrator m = new HiveMigrator(hive);
    pMover.getDependentMovers().clear();
    pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
    try {
      m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
    } catch (Exception e) {
      //Quash
    }
    //Directory still points destination
    assertNotNull(Filter.grepItemAgainstList(destination.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
    //Records exist ondestination
    assertEquals(primaryKey, pMover.get(primaryKey, destination));
    assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, destination));
  }


  private Pair<Node, Node> initializeTestData(Hive hive, String primaryKey, Integer secondaryKey) throws Exception {
    hive.directory().insertPrimaryIndexKey(primaryKey);
    //Setup the test data on one node
    NodeResolver dir = new DbDirectory(hive.getHiveConfiguration(), getDataSource(getConnectString(getHiveDatabaseName())));
    int originId = Atom.getFirst(dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey)).getNodeId();
    Node origin = hive.getNode(originId);
    Node destination = origin.getName().equals("data1") ? hive.getNode("data2") :
      hive.getNode("data1");
    PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
    Mover<Integer> secMover = new SecondaryMover();
    pMover.copy(primaryKey, origin);
    secMover.copy(secondaryKey, origin);
    return new Pair<Node, Node>(origin, destination);
  }

  public static class TestMigrationSchema extends Schema {
    private static TestMigrationSchema INSTANCE = new TestMigrationSchema();

    private TestMigrationSchema() {
      super("testMigration");
    }

    @Override
    public Collection<TableInfo> getTables(String uri) {
      return Arrays.asList(
        new TableInfo("primary_table", "create table primary_table (id varchar(50));"),
        new TableInfo("secondary_table", "create table secondary_table (id integer);"));
    }

    public static TestMigrationSchema getInstance() {
      return INSTANCE;
    }
  }

  class PrimaryMover implements PartitionKeyMover<String> {
    @SuppressWarnings("unchecked")
    private Collection<Entry<Mover, KeyLocator>> movers;
    private String originUri;

    @SuppressWarnings("unchecked")
    public PrimaryMover(String uri) {
      this.originUri = uri;
      movers = new ArrayList<Entry<Mover, KeyLocator>>();
      movers.add(new Pair<Mover, KeyLocator>(new SecondaryMover(), new SecondaryKeyLocator(originUri)));
    }

    @SuppressWarnings("unchecked")
    public Collection<Entry<Mover, KeyLocator>> getDependentMovers() {
      return movers;
    }

    public void copy(String item, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      dao.getJdbcTemplate().update("insert into primary_table values (?)", new Object[]{item});
    }

    public void delete(String item, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      dao.getJdbcTemplate().update("delete from primary_table where id = ?", new Object[]{item});
    }

    public String get(Object id, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      return (String) dao.getJdbcTemplate().queryForObject("select id from primary_table where id = ?", new Object[]{id}, String.class);
    }

  }

  class SecondaryKeyLocator implements KeyLocator<String, Integer> {
    private String uri;

    public SecondaryKeyLocator(String uri) {
      this.uri = uri;
    }

    @SuppressWarnings("unchecked")
    public Collection<Integer> findAll(String parent) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
      return dao.getJdbcTemplate().queryForList("select id from secondary_table", Integer.class);
    }

  }

  class SecondaryMover implements Mover<Integer> {

    public void copy(Integer item, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
    }

    public void delete(Integer item, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      dao.getJdbcTemplate().update("delete from secondary_table where id = ?", new Object[]{item});
    }

    public Integer get(Object id, Node node) {
      SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
      dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
      return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
    }

  }
}
