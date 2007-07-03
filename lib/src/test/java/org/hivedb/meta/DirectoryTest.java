package org.hivedb.meta;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectoryTest extends HiveTestCase {
	private PartitionDimension dimension;
	private SecondaryIndex nameIndex;
	private String secondaryKey = "secondary key";

	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = getHive();
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		for(String name: getDatabaseNames())
			hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(name));
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
		
		dimension = hive.getPartitionDimension(createPopulatedPartitionDimension().getName());
		nameIndex = new SecondaryIndex(new ColumnInfo("name", Types.VARCHAR));
		hive.addSecondaryIndex(Atom.getFirstOrNull(dimension.getResources()), nameIndex);
	}

	private void insertKeys(Hive hive) throws HiveReadOnlyException {
		for(Integer key: getPrimaryIndexKeys()) {
			hive.insertPrimaryIndexKey(dimension, key);
			hive.insertSecondaryIndexKey(nameIndex, secondaryKey, key);
		}
	}
	
	@Test
	public void testInsertPrimaryIndexKey() throws Exception{
		NodeResolver d = getDirectory();
		final Integer key = new Integer(43);
		Node firstNode = Atom.getFirst(dimension.getNodeGroup().getNodes());
		d.insertPrimaryIndexKey( Arrays.asList( new Node[] {Atom.getFirst(dimension.getNodeGroup().getNodes())}), key);
		for(Integer id: d.getNodeIdsOfPrimaryIndexKey(key))
			assertEquals((Integer)firstNode.getId(), id);
	}
	
	@Test
	public void testInsertPrimaryIndexKeyMultipleNodes() throws Exception{
		NodeResolver d = getDirectory();
		final Integer key = new Integer(43);
		d.insertPrimaryIndexKey( dimension.getNodeGroup().getNodes(), key);
		Collection<Integer> nodeIds = d.getNodeIdsOfPrimaryIndexKey(key);
		AssertUtils.assertUnique(nodeIds);
		assertEquals(dimension.getNodeGroup().getNodes().size(), nodeIds.size());
	}
	
	@Test
	public void testDeletePrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testDeletePrimaryIndexKeyMultipleNodes() throws Exception {
		NodeResolver d = getDirectory();
		for(Integer key: getPrimaryIndexKeys())
			d.insertPrimaryIndexKey(d.getPartitionDimension().getNodeGroup().getNodes(), key);
		for(Integer key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testGetNodeIdsOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer key : getPrimaryIndexKeys())
			assertEquals(1, d.getNodeIdsOfPrimaryIndexKey(key).size());
	}

	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
	
	@Test
	public void testUpdatePrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer key: getPrimaryIndexKeys()){
			assertEquals(1, d.getNodeIdsOfPrimaryIndexKey(key).size());
			d.updatePrimaryIndexKey(d.getPartitionDimension().getNodeGroup().getNodes(), key);
			assertEquals(d.getPartitionDimension().getNodeGroup().getNodes().size(), d.getNodeIdsOfPrimaryIndexKey(key).size());
			AssertUtils.assertUnique(d.getNodeIdsOfPrimaryIndexKey(key));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKeys() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		AssertUtils.assertUnique(d.getNodeIdsOfSecondaryIndexKey(nameIndex, secondaryKey));
	}
	
	@Test
	public void testGetNodeSemaphoresOfSecondaryIndexKey() throws Exception{
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		assertEquals(getPrimaryIndexKeys().size(), d.getNodeSemaphoresOfSecondaryIndexKey(nameIndex, secondaryKey).size());
	}
	
	@Test
	public void testGetPrimaryIndexKeysOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		assertEquals(getPrimaryIndexKeys().size(), d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKey).size());
	}
	
	@Test
	public void testUpdateSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		Hive hive = getHive();
		Integer newPKey = new Integer(45);
		hive.insertPrimaryIndexKey(dimension, newPKey);
		
		Collection<Object> pKeys = d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKey);
		Object oldPKey = Atom.getFirst(pKeys);
		d.updatePrimaryIndexOfSecondaryKey(nameIndex, secondaryKey, oldPKey, newPKey);
		Collection<Object> newPKeys = d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKey);
		assertTrue(!pKeys.contains(newPKey));
		assertTrue(newPKeys.contains(newPKey));
		assertTrue(!newPKeys.contains(oldPKey));
	}
	
	
	@Test
	public void testUpdatePrimaryIndexKeyReadOnly() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()){
			d.updatePrimaryIndexKeyReadOnly(key, true);
			for(NodeSemaphore s : d.getNodeSemamphoresOfPrimaryIndexKey(key))
				assertTrue(s.isReadOnly());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdatePrimaryIndexOfSecondaryKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		Iterator<Integer> itr = getPrimaryIndexKeys().iterator();
		Integer key1 = itr.next();
		Integer key2 = itr.next();
		d.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2);
		Collection secondaryKeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key1);
		assertEquals(1, secondaryKeys.size());
		assertEquals(0, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2).size());
		d.updatePrimaryIndexOfSecondaryKey(nameIndex, Atom.getFirst(secondaryKeys), key1, key2);
		assertEquals(0, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key1).size());
		assertEquals(1, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2).size());
	}
	
	@Test
	public void testDeleteAllSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer key: getPrimaryIndexKeys()){
			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() > 0);
			d.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(nameIndex,key);
			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() == 0);
		}
	}
	
	@Test
	public void testDeleteSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer pkey: getPrimaryIndexKeys()){
			Collection secondaryKeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			assertTrue(secondaryKeys.size() > 0);
			for(Object skey : secondaryKeys)
				d.deleteSecondaryIndexKey(nameIndex, skey, pkey);
			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey).size() == 0);
		}
	}
	
	@Test
	public void testDoesPrimaryIndexKeyExist() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		assertTrue(d.doesPrimaryIndexKeyExist(Atom.getFirst(getPrimaryIndexKeys())));
		assertTrue(!d.doesPrimaryIndexKeyExist(new Integer(378465784)));
	}
	
	@Test
	public void testGetNodeSemaphoresOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys())
			assertEquals(1, d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
	}
	
	@Test
	public void testGetNodeSemaphoresOfPrimaryIndexKeyMultiNode() throws Exception {
		NodeResolver d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()) {
			d.insertPrimaryIndexKey(dimension.getNodeGroup().getNodes(), pkey);
			assertEquals(dimension.getNodeGroup().getNodes().size(), d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
		}
	}
	
	@Test
	public void testGetReadOnlyOfPrimaryIndexKey() throws Exception{
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()){
			assertEquals(false, d.getReadOnlyOfPrimaryIndexKey(pkey));
			d.updatePrimaryIndexKeyReadOnly(pkey, true);
			assertTrue(d.getReadOnlyOfPrimaryIndexKey(pkey));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer pkey: getPrimaryIndexKeys()) {
			Collection<String> skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			for(String skey : skeys){
				AssertUtils.assertUnique(d.getNodeIdsOfSecondaryIndexKey(nameIndex, skey));
				assertTrue(d.getNodeIdsOfSecondaryIndexKey(nameIndex, skey).size() > 0);
			}
		}
	}
	
	@Test
	public void testGetNodeSemphoresOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		Collection<NodeSemaphore> skeys = d.getNodeSemaphoresOfSecondaryIndexKey(nameIndex, secondaryKey);
		assertEquals(getPrimaryIndexKeys().size(), skeys.size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		NodeResolver d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()) {
			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			assertTrue(skeys.size() > 0);
			assertEquals(secondaryKey, Atom.getFirst(skeys));
		}
		
	}
	
	private Directory getDirectory() {
		return new Directory(dimension, new HiveBasicDataSource(dimension.getIndexUri()));
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[] {getHiveDatabaseName(), "data1", "data2"});
	}
	
	private Collection<Integer> getPrimaryIndexKeys() {
		return Arrays.asList(new Integer[] {1,2,3,4});
	}
}
