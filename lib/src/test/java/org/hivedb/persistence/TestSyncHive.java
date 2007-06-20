package org.hivedb.persistence;

import java.util.ArrayList;
import java.util.Observer;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveSyncDaemon;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSyncHive extends HiveTestCase {

	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		Node node = createNode(getHiveDatabaseName());
		node.setName("firstNode");
		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), node);
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
	}
	
	@Test
	public void testDaemonSync() throws Exception {
		Hive hive = loadHive();
		Hive passiveSync = loadHive();
		ArrayList<Observer> observers = new ArrayList<Observer>();
		observers.add(passiveSync);
		HiveSyncDaemon daemon = new HiveSyncDaemon(getConnectString(getHiveDatabaseName()), observers);
		daemon.detectChanges();
		
		hive.addNode(Atom.getFirstOrNull(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
		
		daemon.detectChanges();
		
//		nodeReport(passiveSync, hive);
		
		Assert.assertNotNull(Atom.getFirstOrNull(passiveSync.getPartitionDimensions()).getNodeGroup().getNode(createNode(getHiveDatabaseName()).getName()));
	}
	
	@Test
	public void testSync() throws Exception {
		Hive hive = loadHive();
		hive.addNode(Atom.getFirstOrNull(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
		
		Assert.assertNotNull(Atom.getFirstOrNull(hive.getPartitionDimensions()).getNodeGroup().getNode(createNode(getHiveDatabaseName()).getName()));
	}
	
	@SuppressWarnings("unused")
	private void nodeReport(Hive passiveSync, Hive hive) {
		System.out.println("Passively synced Hive:" + passiveSync.getRevision());
		for(Node node: passiveSync.getPartitionDimension(createPopulatedPartitionDimension().getName()).getNodeGroup().getNodes())
			System.out.println(node.getName());
		System.out.println("In-memory Hive " + hive.getRevision());
		for(Node node: hive.getPartitionDimension(createPopulatedPartitionDimension().getName()).getNodeGroup().getNodes())
			System.out.println(node.getName());
	}
	
	private Hive loadHive() throws HiveException {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
}
