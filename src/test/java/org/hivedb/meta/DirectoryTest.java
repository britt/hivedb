package org.hivedb.meta;

import static org.hivedb.meta.directory.DirectoryWrapper.semaphoreToId;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.HiveLockableException;
import org.hivedb.Lockable.Status;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Transform;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectoryTest extends H2TestCase {
	private PartitionDimension dimension;
	private SecondaryIndex nameIndex, numIndex;
	private String secondaryKeyString = "secondary key";
	private Integer secondaryKeyNum = 1;
	private Resource resource;
	
	@BeforeMethod
	public void setUp() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		HiveFacade hive = Hive.create(getConnectString(H2TestCase.TEST_DB), partitionDimensionName(), Types.INTEGER);
		hive.addResource(createResource());
		nameIndex = new SecondaryIndex("name", Types.VARCHAR);
		numIndex = new SecondaryIndex("num", Types.INTEGER);
		resource = hive.getPartitionDimension().getResource(createResource().getName());
		hive.addSecondaryIndex(resource, nameIndex);
		hive.addSecondaryIndex(resource, numIndex);
		resource = hive.getPartitionDimension().getResource(resource.getName());
		dimension = hive.getPartitionDimension();
		hive.addNode(new Node("node", getConnectString(H2TestCase.TEST_DB), "",HiveDbDialect.H2));
	}

	
	protected Resource createResource() {
		final Resource resource = new Resource("FOO", Types.INTEGER, false);
		resource.setPartitionDimension(createEmptyPartitionDimension());
		return resource;
	}


	
	protected PartitionDimension createPopulatedPartitionDimension() {
		return new PartitionDimension(Hive.NEW_OBJECT_ID, partitionDimensionName(), Types.INTEGER,
				getConnectString(H2TestCase.TEST_DB), createResources());
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return new PartitionDimension(Hive.NEW_OBJECT_ID, partitionDimensionName(), Types.INTEGER,
				getConnectString(H2TestCase.TEST_DB), new ArrayList<Resource>());
	}
	protected String partitionDimensionName() {
		return "member";
	}
	protected Collection<Resource> createResources() {
		ArrayList<Resource> resources = new ArrayList<Resource>();
		resources.add(createResource());
		return resources;
	}
	private Hive getHive() {
		return Hive.load(getConnectString(H2TestCase.TEST_DB));
	}


	@Test
	public void testInsertPrimaryIndexKey() throws Exception{
		Directory d = getDirectory();
		Integer key = new Integer(43);
		HiveFacade hive = getHive();
		Node firstNode = Atom.getFirst(hive.getNodes());
		d.insertPrimaryIndexKey( Atom.getFirst(hive.getNodes()), key);
		for(Integer id: Transform.map(semaphoreToId(), d.getKeySemamphoresOfPrimaryIndexKey(key)))
			assertEquals((Integer)firstNode.getId(), id);
	}
	
	@Test
	public void testInsertPrimaryIndexKeyMultipleNodes() throws Exception{
		Directory d = getDirectory();
		HiveFacade hive = getHive();
		Integer key = new Integer(43);
		for(Node node : hive.getNodes())
			d.insertPrimaryIndexKey( node, key);
		Collection<Integer> nodeIds = Transform.map(semaphoreToId(), d.getKeySemamphoresOfPrimaryIndexKey(key));
		AssertUtils.assertUnique(nodeIds);
		assertEquals(hive.getNodes().size(), nodeIds.size());
	}
	
	@Test
	public void testDeletePrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getKeySemamphoresOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testDeletePrimaryIndexKeyMultipleNodes() throws Exception {
		Directory d = getDirectory();
		HiveFacade hive = getHive();
		for(String key: getPrimaryIndexKeys())
			for(Node node : hive.getNodes())
			d.insertPrimaryIndexKey(node, key);
		for(String key : getPrimaryIndexKeys()){
			d.deletePrimaryIndexKey(key);
			assertEquals(0,d.getKeySemamphoresOfPrimaryIndexKey(key).size());
		}
	}
	
	@Test
	public void testGetNodeIdsOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys())
			assertEquals(1, d.getKeySemamphoresOfPrimaryIndexKey(key).size());
	}


	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKeys() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		assertTrue(d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString).size() >= 1);
	}
	
	@Test
	public void testGetKeySemaphoresOfSecondaryIndexKey() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		assertEquals(1, d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfResourceIds() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys())
			assertEquals(1, d.getKeySemaphoresOfResourceId(resource, key).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfPartitioningResourceIds() throws Exception{
		HiveFacade hive = Hive.load(getConnectString(H2TestCase.TEST_DB));
		hive.deleteResource(resource);
		resource = Atom.getFirstOrNull(dimension.getResources());
		resource.setIsPartitioningResource(true);
		hive.addResource(resource);
		
		resource = hive.getPartitionDimension().getResource(resource.getName());
		
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys())
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
		HiveFacade hive = getHive();
		for(String key :  getPrimaryIndexKeys()) {
			d.insertPrimaryIndexKey(Atom.getFirstOrThrow(hive.getNodes()), key);
			d.insertResourceId(resource, key+1, key);
			assertEquals(key, Atom.getFirstOrThrow(d.getPrimaryIndexKeysOfSecondaryIndexKey(resource.getIdIndex(), key+1)).toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInsertRelatedSecondaryIndexKeys() throws Exception {
		Hive hive = getHive();
		Directory d = getDirectory();
		for(String primaryIndexKey: getPrimaryIndexKeys()) {
			hive.directory().insertPrimaryIndexKey(primaryIndexKey);
			d.insertResourceId(resource, primaryIndexKey, primaryIndexKey);
			
			Map<SecondaryIndex, Collection<Object>> secondaryIndexKeyMap = new Hashtable<SecondaryIndex, Collection<Object>>();
			secondaryIndexKeyMap.put(nameIndex, Arrays.asList(new Object[] {
					secondaryKeyString
			}));
			secondaryIndexKeyMap.put(numIndex, Arrays.asList(new Object[] {
					secondaryKeyNum
			}));
			d.batch().insertSecondaryIndexKeys(secondaryIndexKeyMap, primaryIndexKey);
			assertEquals(1,d.getSecondaryIndexKeysOfResourceId(nameIndex, primaryIndexKey).size());
			assertEquals(secondaryKeyString, Atom.getFirst(d.getSecondaryIndexKeysOfResourceId(nameIndex, primaryIndexKey)));
			assertEquals(1,
					d.getSecondaryIndexKeysOfResourceId(numIndex, primaryIndexKey).size());
			assertEquals(secondaryKeyNum, 
					Atom.getFirst(d.getSecondaryIndexKeysOfResourceId(numIndex, primaryIndexKey)));
		}
	}
	
	@Test
	public void testUpdatePrimaryIndexKeyReadOnly() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys()){
			d.updatePrimaryIndexKeyReadOnly(key, true);
			for(KeySemaphore s : d.getKeySemamphoresOfPrimaryIndexKey(key))
				assertTrue(s.getStatus().equals(Status.readOnly));
		}
	}
	
	@Test 
	public void testGetAllSecondaryIndexKeysOdPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key: getPrimaryIndexKeys())
			assertTrue(d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, key).size() > 0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String pkey: getPrimaryIndexKeys()){
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
		for(String pkey : getPrimaryIndexKeys())
			assertEquals(1, d.getKeySemamphoresOfPrimaryIndexKey(pkey).size());
	}
	
	@Test
	public void testGetKeySemaphoresOfPrimaryIndexKeyMultiNode() throws Exception {
		Directory d = getDirectory();
		HiveFacade hive = getHive();
		for(String pkey : getPrimaryIndexKeys()) {
			for(Node node : hive.getNodes())
				d.insertPrimaryIndexKey(node, pkey);
			assertEquals(hive.getNodes().size(), d.getKeySemamphoresOfPrimaryIndexKey(pkey).size());
		}
	}
	
	@Test
	public void testGetReadOnlyOfPrimaryIndexKey() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String pkey : getPrimaryIndexKeys()){
			assertEquals(false, Lists.or(Transform.map(DirectoryWrapper.semaphoreToReadOnly(), d.getKeySemamphoresOfPrimaryIndexKey(pkey))));
			d.updatePrimaryIndexKeyReadOnly(pkey, true);
			assertTrue(Lists.or(Transform.map(DirectoryWrapper.semaphoreToReadOnly(), d.getKeySemamphoresOfPrimaryIndexKey(pkey))));
		}
	}
	
	@Test
	public void testGetReadOnlyOfResourceId() throws Exception{
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String pkey : getPrimaryIndexKeys()){
			assertEquals(false, Lists.or(Transform.map(DirectoryWrapper.semaphoreToReadOnly(), d.getKeySemaphoresOfResourceId(resource, pkey))));
			d.updatePrimaryIndexKeyReadOnly(pkey, true);
			assertTrue(Lists.or(Transform.map(DirectoryWrapper.semaphoreToReadOnly(), d.getKeySemaphoresOfResourceId(resource, pkey))));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetNodeIdsOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String pkey: getPrimaryIndexKeys()) {
			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			for(Object skey : skeys){
				assertTrue(d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, skey).size() > 0);
			}
		}
	}
	
	@Test
	public void testGetNodeSemphoresOfSecondaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		Collection<KeySemaphore> skeys = d.getKeySemaphoresOfSecondaryIndexKey(nameIndex, secondaryKeyString);
		assertEquals(1, skeys.size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetSecondaryIndexKeysOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String pkey : getPrimaryIndexKeys()) {
			Collection skeys = d.getSecondaryIndexKeysOfPrimaryIndexKey(nameIndex, pkey);
			assertTrue(skeys.size() > 0);
			assertEquals(secondaryKeyString, Atom.getFirst(skeys));
		}
		
	}
	
	@Test
	public void testDeleteAllSecondaryKeyForResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys()) {
			assertTrue(d.getSecondaryIndexKeysOfResourceId(numIndex, key).size() > 0);
			d.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, key);
			assertEquals(0,d.getSecondaryIndexKeysOfResourceId(numIndex, key).size());
		}
	}
	
	@Test
	public void testGetSecondaryKeyForResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys())
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
			assertTrue(getPrimaryIndexKeys().contains(key.toString()));
	}
	
	@Test
	public void testDeleteResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys()) {
			d.batch().deleteAllSecondaryIndexKeysOfResourceId(resource, key);
			assertEquals(0,d.getSecondaryIndexKeysOfResourceId(numIndex, key).size());
			d.deleteResourceId(resource, key);
			assertFalse(d.doesResourceIdExist(resource, key));
			assertEquals(0, d.getSecondaryIndexKeysOfResourceId(nameIndex, key).size());
			assertEquals(0, d.getSecondaryIndexKeysOfResourceId(numIndex, key).size());
		}
	}
	
	@Test
	public void testUpdatePrimaryIndexKeyOfResourceId() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		String firstKey = Atom.getFirst(getPrimaryIndexKeys());
		for(String key : getPrimaryIndexKeys()) {
			d.updatePrimaryIndexKeyOfResourceId(resource, key, firstKey);
			assertEquals(firstKey,d.getPrimaryIndexKeyOfResourceId(resource, key).toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetResourceIdsOfPrimaryIndexKey() throws Exception {
		insertKeys(getHive());
		Directory d = getDirectory();
		for(String key : getPrimaryIndexKeys()) {
			assertEquals(1, d.getResourceIdsOfPrimaryIndexKey(resource, key).size());
			assertEquals(key, Atom.getFirst(d.getResourceIdsOfPrimaryIndexKey(resource, key)).toString());
		}
	}
	
	private Directory getDirectory() {
		return new Directory(dimension, new HiveBasicDataSource(dimension.getIndexUri()));
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[] {H2TestCase.TEST_DB, "data1", "data2"});
	}
	
	private Collection<String> getPrimaryIndexKeys() {
		return Arrays.asList(new String[] {"1","2","3","4"});
	}
	
	private void insertKeys(Hive hive) throws HiveLockableException {
		Directory d = getDirectory();
		Resource resource = dimension.getResource(createResource().getName());
		for(String key: getPrimaryIndexKeys()) {
			hive.directory().insertPrimaryIndexKey(key);
			d.insertResourceId(resource, key, key);
			hive.directory().insertSecondaryIndexKey(nameIndex.getResource().getName(), nameIndex.getName(), secondaryKeyString, key);
			hive.directory().insertSecondaryIndexKey(numIndex.getResource().getName(), numIndex.getName(), secondaryKeyNum, key);
		}
	}
}
