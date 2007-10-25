package org.hivedb.persistence;

import java.util.ArrayList;
import java.util.Observer;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveSyncDaemon;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSyncHive extends H2HiveTestCase {

	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.create(
				getConnectString(getHiveDatabaseName()), 
				createEmptyPartitionDimension().getName(), 
				createEmptyPartitionDimension().getColumnType());
		Node node = createNode("firstNode");
		hive.addNode(node);
		new IndexSchema(hive.getPartitionDimension()).install();
	}
	
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
		
		Assert.assertNotNull(passiveSync.getPartitionDimension().getNode(createNode(getHiveDatabaseName()).getName()));
	}
	
	@SuppressWarnings("unused")
	private void nodeReport(Hive passiveSync, Hive hive) {
		System.out.println("Passively synced Hive:" + passiveSync.getRevision());
		for(Node node: passiveSync.getPartitionDimension().getNodes())
			System.out.println(node.getName());
		System.out.println("In-memory Hive " + hive.getRevision());
		for(Node node: hive.getPartitionDimension().getNodes())
			System.out.println(node.getName());
	}
	
	private Hive loadHive() throws HiveException {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
}
