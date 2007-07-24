package org.hivedb.meta;
// Commented out because it takes a long time to run.
/*
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;

import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
*/
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
public class DirectoryTest extends HiveTestCase {
//	private PartitionDimension dimension;
//	private SecondaryIndex nameIndex, numIndex;
//	private String secondaryKeyString = "secondary key";
//	private Integer secondaryKeyNum = 1;
//
//	@BeforeMethod
//	public void setUp() throws Exception {
//		Hive hive = getHive();
//		hive.addPartitionDimension(createPopulatedPartitionDimension());
//		for(String name: getDatabaseNames())
//			hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(name));
//		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
//		
//		dimension = hive.getPartitionDimension(createPopulatedPartitionDimension().getName());
//		nameIndex = new SecondaryIndex("name", Types.VARCHAR);
//		numIndex = new SecondaryIndex("num", Types.INTEGER);
//		hive.addSecondaryIndex(Atom.getFirstOrNull(dimension.getResources()), nameIndex);
//		hive.addSecondaryIndex(Atom.getFirstOrNull(dimension.getResources()), numIndex);
//	}
//
//	private void insertKeys(Hive hive) throws HiveReadOnlyException {
//		for(Integer key: getPrimaryIndexKeys()) {
//			hive.insertPrimaryIndexKey(dimension.getName(), key);
//			hive.insertSecondaryIndexKey(nameIndex.getName(),nameIndex.getResource().getName(), dimension.getName(), secondaryKeyString, key);
//			hive.insertSecondaryIndexKey(numIndex.getName(),numIndex.getResource().getName(), dimension.getName(), secondaryKeyNum, key);
//		}
//	}
//	
//	@Test
//	public void testInsertPrimaryIndexKey() throws Exception{
//		NodeResolver d = getDirectory();
//		final Integer key = new Integer(43);
//		Node firstNode = Atom.getFirst(dimension.getNodes());
//		d.insertPrimaryIndexKey( Atom.getFirst(dimension.getNodes()), key);
//		for(Integer id: d.getNodeIdsOfPrimaryIndexKey(key))
//			assertEquals((Integer)firstNode.getId(), id);
//	}
//	
//	@Test
//	public void testInsertPrimaryIndexKeyMultipleNodes() throws Exception{
//		NodeResolver d = getDirectory();
//		final Integer key = new Integer(43);
//		for(Node node : dimension.getNodes())
//			d.insertPrimaryIndexKey( node, key);
//		Collection<Integer> nodeIds = d.getNodeIdsOfPrimaryIndexKey(key);
//		AssertUtils.assertUnique(nodeIds);
//		assertEquals(dimension.getNodes().size(), nodeIds.size());
//	}
//	
//	@Test
//	public void testDeletePrimaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer key : getPrimaryIndexKeys()){
//			d.deletePrimaryIndexKey(key);
//			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
//		}
//	}
//	
//	@Test
//	public void testDeletePrimaryIndexKeyMultipleNodes() throws Exception {
//		NodeResolver d = getDirectory();
//		for(Integer key: getPrimaryIndexKeys())
//			for(Node node : d.getPartitionDimension().getNodes())
//			d.insertPrimaryIndexKey(node, key);
//		for(Integer key : getPrimaryIndexKeys()){
//			d.deletePrimaryIndexKey(key);
//			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
//		}
//	}
//	
//	@Test
//	public void testGetNodeIdsOfPrimaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer key : getPrimaryIndexKeys())
//			assertEquals(1, d.getNodeIdsOfPrimaryIndexKey(key).size());
//	}
//
//	private Hive getHive() {
//		return Hive.load(getConnectString(getHiveDatabaseName()));
//	}
//	
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testGetNodeIdsOfSecondaryIndexKeys() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		AssertUtils.assertUnique(d.getNodeIdsOfSecondaryIndexKey(nameIndex, secondaryKeyString));
//	}
//	
//	@Test
//	public void testGetNodeSemaphoresOfSecondaryIndexKey() throws Exception{
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		assertEquals(getPrimaryIndexKeys().size(), d.getNodeSemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString).size());
//	}
//	
//	@Test
//	public void testGetPrimaryIndexKeysOfSecondaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		assertEquals(getPrimaryIndexKeys().size(), d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKeyString).size());
//	}
//	
//	@Test
//	public void testInsertRelatedSecondaryIndexKeys() throws Exception {
//		Hive hive = getHive();
//		for(Integer primaryIndexKey: getPrimaryIndexKeys()) {
//			hive.insertPrimaryIndexKey(dimension.getName(), primaryIndexKey);
//			
//			Map<SecondaryIndex, Collection<Object>> secondaryIndexKeyMap = new Hashtable<SecondaryIndex, Collection<Object>>();
//			secondaryIndexKeyMap.put(nameIndex, Arrays.asList(new Object[] {
//					secondaryKeyString
//			}));
//			secondaryIndexKeyMap.put(numIndex, Arrays.asList(new Object[] {
//					secondaryKeyNum
//			}));
//			hive.insertRelatedSecondaryIndexKeys(dimension.getName(), secondaryIndexKeyMap, primaryIndexKey);
//			assertEquals(1,hive.getSecondaryIndexKeysWithPrimaryKey(nameIndex.getName(), nameIndex.getResource().getName(), dimension.getName(), primaryIndexKey).size());
//			assertEquals(1,hive.getSecondaryIndexKeysWithPrimaryKey(numIndex.getName(), nameIndex.getResource().getName(),dimension.getName(), primaryIndexKey).size());
//		}
//	}
//	
//	@Test
//	public void testUpdateSecondaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		Hive hive = getHive();
//		Integer newPKey = new Integer(45);
//		hive.insertPrimaryIndexKey(dimension.getName(), newPKey);
//		
//		Collection<Object> pKeys = d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKeyString);
//		Object oldPKey = Atom.getFirst(pKeys);
//		d.updatePrimaryIndexOfSecondaryKey(nameIndex, secondaryKeyString, oldPKey, newPKey);
//		Collection<Object> newPKeys = d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKeyString);
//		assertTrue(!pKeys.contains(newPKey));
//		assertTrue(newPKeys.contains(newPKey));
//		assertTrue(!newPKeys.contains(oldPKey));
//	}
//	
//	
//	@Test
//	public void testUpdatePrimaryIndexKeyReadOnly() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer key : getPrimaryIndexKeys()){
//			d.updatePrimaryIndexKeyReadOnly(key, true);
//			for(NodeSemaphore s : d.getNodeSemamphoresOfPrimaryIndexKey(key))
//				assertTrue(s.isReadOnly());
//		}
//	}
//	
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testUpdatePrimaryIndexOfSecondaryKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		Iterator<Integer> itr = getPrimaryIndexKeys().iterator();
//		Integer key1 = itr.next();
//		Integer key2 = itr.next();
//		d.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2);
//		Collection secondaryKeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key1);
//		assertEquals(1, secondaryKeys.size());
//		assertEquals(0, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2).size());
//		d.updatePrimaryIndexOfSecondaryKey(nameIndex, Atom.getFirst(secondaryKeys), key1, key2);
//		assertEquals(0, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key1).size());
//		assertEquals(1, d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key2).size());
//	}
//	
//	@Test
//	public void testDeleteAllSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer key: getPrimaryIndexKeys()){
//			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() > 0);
//			d.deleteAllSecondaryIndexKeysOfPrimaryIndexKey(nameIndex,key);
//			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() == 0);
//		}
//	}
//	
//	@Test
//	public void testDeleteSecondaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer pkey: getPrimaryIndexKeys()){
//			Collection secondaryKeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
//			assertTrue(secondaryKeys.size() > 0);
//			for(Object skey : secondaryKeys)
//				d.deleteSecondaryIndexKey(nameIndex, skey, pkey);
//			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey).size() == 0);
//		}
//	}
//	
//	@Test
//	public void testDoesPrimaryIndexKeyExist() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		assertTrue(d.doesPrimaryIndexKeyExist(Atom.getFirst(getPrimaryIndexKeys())));
//		assertTrue(!d.doesPrimaryIndexKeyExist(new Integer(378465784)));
//	}
//	
//	@Test
//	public void testGetNodeSemaphoresOfPrimaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer pkey : getPrimaryIndexKeys())
//			assertEquals(1, d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
//	}
//	
//	@Test
//	public void testGetNodeSemaphoresOfPrimaryIndexKeyMultiNode() throws Exception {
//		NodeResolver d = getDirectory();
//		for(Integer pkey : getPrimaryIndexKeys()) {
//			for(Node node : dimension.getNodes())
//				d.insertPrimaryIndexKey(node, pkey);
//			assertEquals(dimension.getNodes().size(), d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
//		}
//	}
//	
//	@Test
//	public void testGetReadOnlyOfPrimaryIndexKey() throws Exception{
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer pkey : getPrimaryIndexKeys()){
//			assertEquals(false, d.getReadOnlyOfPrimaryIndexKey(pkey));
//			d.updatePrimaryIndexKeyReadOnly(pkey, true);
//			assertTrue(d.getReadOnlyOfPrimaryIndexKey(pkey));
//		}
//	}
//	
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testGetNodeIdsOfSecondaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer pkey: getPrimaryIndexKeys()) {
//			Collection<String> skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
//			for(String skey : skeys){
//				AssertUtils.assertUnique(d.getNodeIdsOfSecondaryIndexKey(nameIndex, skey));
//				assertTrue(d.getNodeIdsOfSecondaryIndexKey(nameIndex, skey).size() > 0);
//			}
//		}
//	}
//	
//	@Test
//	public void testGetNodeSemphoresOfSecondaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		Collection<NodeSemaphore> skeys = d.getNodeSemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString);
//		assertEquals(getPrimaryIndexKeys().size(), skeys.size());
//	}
//	
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testGetSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
//		insertKeys(getHive());
//		NodeResolver d = getDirectory();
//		for(Integer pkey : getPrimaryIndexKeys()) {
//			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
//			assertTrue(skeys.size() > 0);
//			assertEquals(secondaryKeyString, Atom.getFirst(skeys));
//		}
//		
//	}
//	
//	private Directory getDirectory() {
//		return new Directory(dimension, new HiveBasicDataSource(dimension.getIndexUri()));
//	}
//	
//	@Override
//	public Collection<String> getDatabaseNames() {
//		return Arrays.asList(new String[] {getHiveDatabaseName(), "data1", "data2"});
//	}
//	
//	private Collection<Integer> getPrimaryIndexKeys() {
//		return Arrays.asList(new Integer[] {1,2,3,4});
//	}
}
