package org.hivedb.meta;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectoryTest extends H2HiveTestCase {
	private PartitionDimension dimension;
	private SecondaryIndex nameIndex, numIndex;
	private String secondaryKeyString = "secondary key";
	private Integer secondaryKeyNum = 1;
	private Resource resource;
	
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = getHive();
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		for(String name: getDatabaseNames())
			hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(name));
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
		
		dimension = hive.getPartitionDimension(createPopulatedPartitionDimension().getName());
		nameIndex = new SecondaryIndex("name", Types.VARCHAR);
		numIndex = new SecondaryIndex("num", Types.INTEGER);
		resource = Atom.getFirstOrNull(dimension.getResources());
		hive.addSecondaryIndex(resource, nameIndex);
		hive.addSecondaryIndex(resource, numIndex);
		resource = hive.getPartitionDimension(dimension.getName()).getResource(resource.getName());
	}

	
	@Test
	public void testInsertPrimaryIndexKey() throws Exception{
		Directory d = getDirectory();
		Integer key = new Integer(43);
		Node firstNode = Atom.getFirst(dimension.getNodes());
		d.insertPrimaryIndexKey( Atom.getFirst(dimension.getNodes()), key);
		for(Integer id: d.getNodeIdsOfPrimaryIndexKey(key))
			assertEquals((Integer)firstNode.getId(), id);
	}
	
	@Test
	public void testInsertPrimaryIndexKeyMultipleNodes() throws Exception{
		Directory d = getDirectory();
		Integer key = new Integer(43);
		for(Node node : dimension.getNodes())
			d.insertPrimaryIndexKey( node, key);
		Collection<Integer> nodeIds = d.getNodeIdsOfPrimaryIndexKey(key);
		AssertUtils.assertUnique(nodeIds);
		assertEquals(dimension.getNodes().size(), nodeIds.size());
	}
	
	@Test
	public void testDeletePrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testDeletePrimaryIndexKeyMultipleNodes() throws Exception {
		Directory d = getDirectory();
		for(Integer key: getPrimaryIndexKeys())
			for(Node node : d.getPartitionDimension().getNodes())
			d.insertPrimaryIndexKey(node, key);
		for(Integer key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getNodeIdsOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testGetNodeIdsOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys())
			assertEquals(1, d.getNodeIdsOfPrimaryIndexKey(key).size());
	}


	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKeys() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		assertTrue(d.getNodeIdsOfSecondaryIndexKey(nameIndex, secondaryKeyString).size() >= 1);
	}
	
	@Test
	public void testGetKeySemaphoresOfSecondaryIndexKey() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		assertEquals(getPrimaryIndexKeys().size(), d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfResourceIds() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys())
			assertEquals(1, d.getKeySemaphoresOfResourceId(resource, key).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfPartitioningResourceIds() throws Exception{
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.deleteResource(resource);
		resource = Atom.getFirstOrNull(dimension.getResources());
		resource.setIsPartitioningResource(true);
		hive.addResource(dimension.getName(), resource);
		
		nameIndex = new SecondaryIndex("name", Types.VARCHAR);
		numIndex = new SecondaryIndex("num", Types.INTEGER);
		hive.addSecondaryIndex(resource, nameIndex);
		hive.addSecondaryIndex(resource, numIndex);
		resource = hive.getPartitionDimension(dimension.getName()).getResource(resource.getName());
		
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys())
			assertEquals(1, d.getKeySemaphoresOfResourceId(resource, key).size());
	}
	
	@Test
	public void testGetPrimaryIndexKeysOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		assertEquals(getPrimaryIndexKeys().size(), d.getPrimaryIndexKeysOfSecondaryIndexKey(nameIndex, secondaryKeyString).size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetPrimaryIndexKeysOfResourceId() throws Exception {
		Directory d = getDirectory();
		for(Integer key :  getPrimaryIndexKeys()) {
			d.insertPrimaryIndexKey(Atom.getFirstOrThrow(dimension.getNodes()), key);
			d.insertResourceId(resource, key+1, key);
			assertEquals(key, Atom.getFirstOrThrow(d.getPrimaryIndexKeysOfSecondaryIndexKey(resource.getIdIndex(), key+1)));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInsertRelatedSecondaryIndexKeys() throws Exception {
		Hive hive = getHive();
		Directory d = getDirectory();
		for(Integer primaryIndexKey: getPrimaryIndexKeys()) {
			hive.insertPrimaryIndexKey(dimension.getName(), primaryIndexKey);
			d.insertResourceId(resource, primaryIndexKey, primaryIndexKey);
			
			Map<SecondaryIndex, Collection<Object>> secondaryIndexKeyMap = new Hashtable<SecondaryIndex, Collection<Object>>();
			secondaryIndexKeyMap.put(nameIndex, Arrays.asList(new Object[] {
					secondaryKeyString
			}));
			secondaryIndexKeyMap.put(numIndex, Arrays.asList(new Object[] {
					secondaryKeyNum
			}));
			d.batch().insertSecondaryIndexKeys(secondaryIndexKeyMap, primaryIndexKey);
			assertEquals(1,hive.getSecondaryIndexKeysWithResourceId(nameIndex.getName(), nameIndex.getResource().getName(), dimension.getName(), primaryIndexKey).size());
			assertEquals(secondaryKeyString, Atom.getFirst(hive.getSecondaryIndexKeysWithResourceId(nameIndex.getName(), nameIndex.getResource().getName(), dimension.getName(), primaryIndexKey)));
			assertEquals(1,hive.getSecondaryIndexKeysWithResourceId(numIndex.getName(), nameIndex.getResource().getName(),dimension.getName(), primaryIndexKey).size());
			assertEquals(secondaryKeyNum, Atom.getFirst(hive.getSecondaryIndexKeysWithResourceId(numIndex.getName(), numIndex.getResource().getName(), dimension.getName(), primaryIndexKey)));
		}
	}
	
	@Test
	public void testUpdatePrimaryIndexKeyReadOnly() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()){
			d.updatePrimaryIndexKeyReadOnly(key, true);
			for(KeySemaphore s : d.getNodeSemamphoresOfPrimaryIndexKey(key))
				assertTrue(s.isReadOnly());
		}
	}
	
	@Test 
	public void testGetAllSecondaryIndexKeysOdPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key: getPrimaryIndexKeys())
			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() > 0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
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
		Directory d = getDirectory();
		assertTrue(d.doesPrimaryIndexKeyExist(Atom.getFirst(getPrimaryIndexKeys())));
		assertTrue(!d.doesPrimaryIndexKeyExist(new Integer(378465784)));
	}
	
	@Test
	public void testGetKeySemaphoresOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys())
			assertEquals(1, d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfPrimaryIndexKeyMultiNode() throws Exception {
		Directory d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()) {
			for(Node node : dimension.getNodes())
				d.insertPrimaryIndexKey(node, pkey);
			assertEquals(dimension.getNodes().size(), d.getNodeSemamphoresOfPrimaryIndexKey(pkey).size());
		}
	}
	
	@Test
	public void testGetReadOnlyOfPrimaryIndexKey() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()){
			assertEquals(false, d.getReadOnlyOfPrimaryIndexKey(pkey));
			d.updatePrimaryIndexKeyReadOnly(pkey, true);
			assertTrue(d.getReadOnlyOfPrimaryIndexKey(pkey));
		}
	}
	
	@Test
	public void testGetReadOnlyOfResourceId() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()){
			assertEquals(false, d.getReadOnlyOfResourceId(resource, pkey));
			d.updatePrimaryIndexKeyReadOnly(pkey, true);
			assertTrue(d.getReadOnlyOfResourceId(resource, pkey));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer pkey: getPrimaryIndexKeys()) {
			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			for(Object skey : skeys){
				assertTrue(d.getNodeIdsOfSecondaryIndexKey(nameIndex, skey).size() > 0);
			}
		}
	}
	
	@Test
	public void testGetNodeSemphoresOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		Collection<KeySemaphore> skeys = d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString);
		assertEquals(getPrimaryIndexKeys().size(), skeys.size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer pkey : getPrimaryIndexKeys()) {
			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			assertTrue(skeys.size() > 0);
			assertEquals(secondaryKeyString, Atom.getFirst(skeys));
		}
		
	}
	
	@Test
	public void testDeleteAllSecondaryKeyForResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()) {
			assertTrue(d.getSecondaryIndexKeysOfResourceId(numIndex, key).size() > 0);
			d.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, key);
			assertEquals(0,d.getSecondaryIndexKeysOfResourceId(numIndex, key).size());
		}
	}
	
	@Test
	public void testGetSecondaryKeyForResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys())
			assertEquals(1,d.getSecondaryIndexKeysOfResourceId(nameIndex, key).size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetResourceIdForSecondaryKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		
		Collection<Integer> keys = d.getResourceIdsOfSecondaryIndexKey(nameIndex, secondaryKeyString);
		assertEquals(getPrimaryIndexKeys().size(),keys.size());
		for(Integer key : keys)
			assertTrue(getPrimaryIndexKeys().contains(key));
	}
	
	@Test
	public void testDeleteResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()) {
			d.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, key);
			assertEquals(0,d.getSecondaryIndexKeysOfResourceId(numIndex, key).size());
			d.deleteResourceId(resource, key);
			assertFalse(d.doesResourceIdExist(resource, key));
		}
	}
	
	@Test
	public void testUpdatePrimaryIndexKeyOfResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		Integer firstKey = Atom.getFirst(getPrimaryIndexKeys());
		for(Integer key : getPrimaryIndexKeys()) {
			d.updatePrimaryIndexKeyOfResourceId(resource, key, key, firstKey);
			assertEquals(firstKey,d.getPrimaryIndexKeyOfResourceId(resource, key));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetResourceIdsOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(Integer key : getPrimaryIndexKeys()) {
			assertEquals(1, d.getResourceIdsOfPrimaryIndexKey(resource, key).size());
			assertEquals(key, Atom.getFirst(d.getResourceIdsOfPrimaryIndexKey(resource, key)));
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
	
	private void insertKeys(Hive hive) throws HiveReadOnlyException {
		Directory d = getDirectory();
		Resource resource = Atom.getFirstOrNull(dimension.getResources());
		for(Integer key: getPrimaryIndexKeys()) {
			hive.insertPrimaryIndexKey(dimension.getName(), key);
			d.insertResourceId(resource, key, key);
			hive.insertSecondaryIndexKey(nameIndex.getName(),nameIndex.getResource().getName(), dimension.getName(), secondaryKeyString, key);
			hive.insertSecondaryIndexKey(numIndex.getName(),numIndex.getResource().getName(), dimension.getName(), secondaryKeyNum, key);
		}
	}
	
	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
}
