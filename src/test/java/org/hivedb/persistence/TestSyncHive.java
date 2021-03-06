package org.hivedb.persistence;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveSyncDaemon;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Observer;

@Config("hive_default")
public class TestSyncHive extends HiveTest {

  /**
   * Load two Hive instances, use one to update the hive, and have the other listen for updates
   * from the HiveSyncDaeon.
   */
  @Test
  public void testDaemonSync() throws Exception {
    Hive hive = loadHive();
    Hive passiveSync = loadHive();
    ArrayList<Observer> observers = new ArrayList<Observer>();
    observers.add(passiveSync);
    HiveSyncDaemon daemon = new HiveSyncDaemon(getConnectString(getHiveDatabaseName()), observers);
    daemon.detectChanges();

    hive.addNode(createNode(getHiveDatabaseName()));

    daemon.detectChanges();

//		nodeReport(passiveSync, hive);

    Assert.assertNotNull(passiveSync.getNode(createNode(getHiveDatabaseName()).getName()));
  }

  @SuppressWarnings("unused")
  private void nodeReport(Hive passiveSync, Hive hive) {
    System.out.println("Passively synced Hive:" + passiveSync.getRevision());
    for (Node node : passiveSync.getNodes())
      System.out.println(node.getName());
    System.out.println("In-memory Hive " + hive.getRevision());
    for (Node node : hive.getNodes())
      System.out.println(node.getName());
  }

  private Hive loadHive() throws HiveException {
    return Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
  }
}
