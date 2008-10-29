package org.hivedb;

import org.hivedb.Lockable.Status;
import org.hivedb.AccessType;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.PartitionDimension;
import org.hivedb.directory.DbDirectory;
import org.hivedb.directory.DirectoryWrapper;
import org.hivedb.directory.NodeResolver;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.functional.Toss;
import org.hivedb.util.functional.Transform;
import org.junit.Test;

@Config("hive_default")
public class TestConnectionWriteLocking extends HiveTest {

  @Test
  public void testHiveLockingInMemory() throws Exception {
    final Hive hive = getHive();
    final String key = new String("North America");

    hive.directory().insertPrimaryIndexKey(key);
    hive.updateHiveStatus(Status.readOnly);

    AssertUtils.assertThrows(new Toss() {

      public void f() throws Exception {
        hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
      }
    }, HiveLockableException.class);
  }

  @Test
  public void testHiveLockingPersistent() throws Exception {
//    Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    final String key = new String("Stoatia");

    hive.directory().insertPrimaryIndexKey(key);
    hive.updateHiveStatus(Status.readOnly);
    hive = null;

//    final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());

    AssertUtils.assertThrows(new Toss() {

      public void f() throws Exception {
//        fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
        throw new UnsupportedOperationException("Not yet implemented");
      }
    }, HiveLockableException.class);
  }

  @Test
  public void testNodeLockingInMemory() throws Exception {
    throw new UnsupportedOperationException("Not yet implemented");
//    final Hive hive = null; //= Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
//    final String key = new String("Antarctica");
//
//    final PartitionDimension partitionDimension = hive.getPartitionDimension();
//    hive.directory().insertPrimaryIndexKey(key);
//    NodeResolver directory = new DbDirectory(hive.getHiveConfiguration(), CachingDataSourceProvider.getInstance().getDataSource(hive.getHiveConfiguration().getUri()));
//    for (Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
//      hive.getNode(id).setStatus(Status.readOnly);
//
//    AssertUtils.assertThrows(new Toss() {
//      public void f() throws Exception {
//        hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
//      }
//    }, HiveLockableException.class);
  }

  @Test
  public void testNodeLockingPersistent() throws Exception {
//    Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    final String key = new String("Asia");

    PartitionDimension partitionDimension = hive.getPartitionDimension();
    hive.directory().insertPrimaryIndexKey(key);
    NodeResolver directory = new DbDirectory(hive.getHiveConfiguration(), CachingDataSourceProvider.getInstance().getDataSource(hive.getHiveConfiguration().getUri()));
    for (Integer id : Transform.map(DirectoryWrapper.semaphoreToId(), directory.getKeySemamphoresOfPrimaryIndexKey(key)))
      hive.updateNodeStatus(hive.getNode(id), Status.readOnly);
    hive = null;

//    final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());

    AssertUtils.assertThrows(new Toss() {

      public void f() throws Exception {
//        fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
      }
    }, HiveLockableException.class);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  //	@Test
  public void testRecordLockingInMemory() throws Exception {
//    final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    final String key = new String("Atlantis");

    hive.directory().insertPrimaryIndexKey(key);
    hive.directory().updatePrimaryIndexKeyReadOnly(key, true);

    AssertUtils.assertThrows(new Toss() {

      public void f() throws Exception {
        hive.connection().getByPartitionKey(key, AccessType.ReadWrite);
      }
    }, HiveLockableException.class);
  }

  //	@Test
  public void testRecordLockingPersistent() throws Exception {
//    Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
    final String key = new String("Africa");

    hive.directory().insertPrimaryIndexKey(key);
    hive.directory().updatePrimaryIndexKeyReadOnly(key, true);
    hive = null;

//    final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());

    AssertUtils.assertThrows(new Toss() {

      public void f() throws Exception {
//        fetchedHive.connection().getByPartitionKey(key, AccessType.ReadWrite);
      }
    }, HiveLockableException.class);
  }
}
