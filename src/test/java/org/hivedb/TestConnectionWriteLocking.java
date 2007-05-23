package org.hivedb;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.Directory;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.AssertUtils.Toss;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConnectionWriteLocking extends HiveTestCase {
	public TestConnectionWriteLocking() {
		this.cleanupDbAfterEachTest = true;
		this.cleanupOnLoad = true;
	}
	
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.load(getConnectString());
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode());
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
	}
	
	@Test
	public void testHiveLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
		hive.updateHiveReadOnly(true);
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.getConnection(Atom.getFirst(hive.getPartitionDimensions()), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testHiveLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
		hive.updateHiveReadOnly(true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString());
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testNodeLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		final PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
		hive.insertPrimaryIndexKey(partitionDimension, key);
		Directory directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getHiveUri()));
		getNode(partitionDimension,directory.getNodeIdOfPrimaryIndexKey(key)).setReadOnly(true);
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.getConnection(partitionDimension, key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testNodeLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
		hive.insertPrimaryIndexKey(partitionDimension, key);
		Directory directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getHiveUri()));
		Node node = getNode(partitionDimension,directory.getNodeIdOfPrimaryIndexKey(key));
		hive.updateNodeReadOnly(node, true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString());
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
		
	}
	
	@Test
	public void testRecordLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()), key, true);
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.getConnection(Atom.getFirst(hive.getPartitionDimensions()), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testRecordLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString());
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()), key, true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString());
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	private Node getNode(PartitionDimension dim, int id) throws HiveException {
		return dim.getNodeGroup().getNode(id);
	}
}
