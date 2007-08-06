package org.hivedb;

import org.hivedb.util.database.DerbyHiveTestCase;

public class TestJdbcDaoSupportWriteLocking extends DerbyHiveTestCase {
//	@BeforeMethod
//	public void setUp() throws Exception {
//		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		hive.addPartitionDimension(createPopulatedPartitionDimension());
//		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
//		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
//	}
//	
//	@Test
//	public void testHiveLockingInMemory() throws Exception {
//		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
//		hive.updateHiveReadOnly(true);
//		
//		AssertUtils.assertThrows(new Toss() {
//			public void f() throws Exception {
//				hive.getJdbcDaoSupportCache(Atom.getFirst(hive.getPartitionDimensions())).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//	}
//	
//	@Test
//	public void testHiveLockingPersistent() throws Exception {
//		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
//		hive.updateHiveReadOnly(true);
//		hive = null;
//		
//		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
//		
//		AssertUtils.assertThrows(new Toss(){
//
//			public void f() throws Exception {
//				fetchedHive.getJdbcDaoSupportCache(Atom.getFirst(fetchedHive.getPartitionDimensions())).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//	}
//	
//	@Test
//	public void testNodeLockingInMemory() throws Exception {
//		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		final PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
//		hive.insertPrimaryIndexKey(partitionDimension, key);
//		Directory directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getHiveUri()));
//		partitionDimension.getNodeGroup().getNode(directory.getNodeIdOfPrimaryIndexKey(key)).setReadOnly(true);
//		
//		AssertUtils.assertThrows(new Toss(){
//
//			public void f() throws Exception {
//				hive.getJdbcDaoSupportCache(partitionDimension).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//	}
//	
//	@Test
//	public void testNodeLockingPersistent() throws Exception {
//		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		PartitionDimension partitionDimension = Atom.getFirst(hive.getPartitionDimensions());
//		hive.insertPrimaryIndexKey(partitionDimension, key);
//		Directory directory = new Directory(partitionDimension, new HiveBasicDataSource(hive.getHiveUri()));
//		Node node = partitionDimension.getNodeGroup().getNode(directory.getNodeIdOfPrimaryIndexKey(key));
//		hive.updateNodeReadOnly(node, true);
//		
//		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
//		
//		AssertUtils.assertThrows(new Toss(){
//
//			public void f() throws Exception {
//				fetchedHive.getJdbcDaoSupportCache(Atom.getFirst(fetchedHive.getPartitionDimensions())).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//		
//	}
//	
//	@Test
//	public void testRecordLockingInMemory() throws Exception {
//		final Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
//		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()), key, true);
//		
//		AssertUtils.assertThrows(new Toss(){
//
//			public void f() throws Exception {
//				hive.getJdbcDaoSupportCache(Atom.getFirst(hive.getPartitionDimensions())).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//	}
//	
//	@Test
//	public void testRecordLockingPersistent() throws Exception {
//		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
//		final Integer key = new Integer(13);
//		
//		hive.insertPrimaryIndexKey(Atom.getFirst(hive.getPartitionDimensions()), key);
//		hive.updatePrimaryIndexReadOnly(Atom.getFirst(hive.getPartitionDimensions()), key, true);
//		hive = null;
//		
//		final Hive fetchedHive = Hive.load(getConnectString(getHiveDatabaseName()));
//		
//		AssertUtils.assertThrows(new Toss(){
//
//			public void f() throws Exception {
//				fetchedHive.getJdbcDaoSupportCache(Atom.getFirst(fetchedHive.getPartitionDimensions())).get(key, AccessType.ReadWrite);
//			}}, HiveReadOnlyException.class);
//	}
}
