package org.hivedb;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.AssertUtils.Toss;
import org.hivedb.util.database.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConnectionWriteLocking extends H2HiveTestCase {
	
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
	}
	
	@Test
	public void testHiveLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()).getName(), key);
		hive.updateHiveReadOnly(true);
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.getConnection(Atom.getFirst(hive.getPartitionDimensions()).getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testHiveLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()).getName(), key);
		hive.updateHiveReadOnly(true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()).getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testNodeLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		final PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
		hive.insertPrimaryIndexKey(partitionDimension.getName(), key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : directory.getNodeIdsOfPrimaryIndexKey(key))
			getNode(partitionDimension,id).setReadOnly(true);
		
		AssertUtils.assertThrows(new Toss(){
			public void f() throws Exception {
				hive.getConnection(partitionDimension.getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testNodeLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
		hive.insertPrimaryIndexKey(partitionDimension.getName(), key);
		NodeResolver directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getUri()));
		for(Integer id : directory.getNodeIdsOfPrimaryIndexKey(key))
			hive.updateNodeReadOnly(getNode(partitionDimension, id), true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()).getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
		
	}
	
	@Test
	public void testRecordLockingInMemory() throws Exception {
		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()).getName(), key);
		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()).getName(), key, true);
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				hive.getConnection(Atom.getFirst(hive.getPartitionDimensions()).getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	@Test
	public void testRecordLockingPersistent() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		final Integer key = new Integer(13);
		
		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()).getName(), key);
		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()).getName(), key, true);
		hive = null;
		
		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
		
		AssertUtils.assertThrows(new Toss(){

			public void f() throws Exception {
				fetchedHive.getConnection(Atom.getFirst(fetchedHive.getPartitionDimensions()).getName(), key, AccessType.ReadWrite);
			}}, HiveReadOnlyException.class);
	}
	
	private Node getNode(PartitionDimension dim, int id) throws HiveException {
		return dim.getNode(id);
	}
}
