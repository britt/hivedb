package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.InstallHiveIndexSchema;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.scenarioBuilder.AssertUtils;
import org.hivedb.util.scenarioBuilder.Atom;
import org.hivedb.util.scenarioBuilder.Filter;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;
import org.hivedb.util.scenarioBuilder.Predicate;
import org.hivedb.util.scenarioBuilder.PrimaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.RingIteratorable;
import org.hivedb.util.scenarioBuilder.SecondaryIndexIdentifiable;
import org.hivedb.util.scenarioBuilder.Transform;
import org.hivedb.util.scenarioBuilder.Unary;
import org.hivedb.util.scenarioBuilder.Undoable;

public class HiveScenarioTest {
	
	protected Hive hive;
	HiveScenarioConfig hiveScenarioConfig;
	public HiveScenarioTest(HiveScenarioConfig hiveScenarioConfig)
	{
		this.hiveScenarioConfig = hiveScenarioConfig;
		hive = hiveScenarioConfig.getHive();
	}
	public void performTest() throws Exception, HiveException, SQLException {
		HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig);
		validate(hiveScenario);
	}

	protected void validate(HiveScenario hiveScenario) throws HiveException, SQLException {
		validateReadsFromPersistence(hiveScenario);
		validateUpdatesToPersistence(hiveScenario);
		validateReadsFromPersistence(hiveScenario);
		validateDeletesToPersistence(hiveScenario);
		validateReadsFromPersistence(hiveScenario);
	}
	
	@SuppressWarnings("unchecked")
	public void validateReadsFromPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHive();
	
		final HashSet<PartitionDimension> expected = new HashSet<PartitionDimension>(hiveScenario.getCreatedPartitionDimensions());
		final HashSet<PartitionDimension> actual = new HashSet<PartitionDimension>(hive.getPartitionDimensions());
		// Validate our PartitionDimension in memory against those that are in the persistence
		assertEquals(String.format("Expected %s but got %s", expected, actual),
					expected.hashCode(),
					actual.hashCode());
		
		for (PartitionDimension partitionDimension : hiveScenario.getCreatedPartitionDimensions()) {
			PartitionDimension partitionDimensionFromHive = hive.getPartitionDimension(partitionDimension.getName());
			
			// Assert that primary index keys got to the persistence
			for (PrimaryIndexIdentifiable primaryIndexInstance : hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension))
				assertNotNull(hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance()));
			
			// Validate that the hive has the created resources
			assertEquals(new TreeSet<Resource>(hiveScenario.getResourcesOfThisPartitionDimension(partitionDimension)),
						 new TreeSet<Resource>(partitionDimensionFromHive.getResources()));
			
			// Validate that the secondary index keys are in the database with the right primary index key
			for (Resource resource : partitionDimension.getResources()) {
				
				for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {	
					Collection<SecondaryIndexIdentifiable> secondaryIndexInstancesFromHiveScenario = hiveScenario.getSecondaryIndexInstancesForThisPartitionDimensionAndResource(partitionDimension, resource, secondaryIndex);
					
					// Assert that querying for all the secondary index keys of a primary index key returns the right collection
					for (final PrimaryIndexIdentifiable primaryIndexInstance : hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension)) {
						Collection secondaryIndexKeys = hive.getSecondaryIndexKeysWithPrimaryKey(
																	partitionDimension.getName(),
																	resource.getName(),
																	secondaryIndex.getName(),
																	primaryIndexInstance.getIdAsPrimaryIndexInstance());
						assertEquals(
							new TreeSet<Object>(
								Transform.map(new Unary<SecondaryIndexIdentifiable, Object>() {
									public Object f(SecondaryIndexIdentifiable secondaryIndexInstance) {
										return secondaryIndexInstance.getIdAsSecondaryIndexInstance(); }},
									filterSecondaryIndexInstancesOfPrimaryIndexInstance(secondaryIndexInstancesFromHiveScenario, primaryIndexInstance))),								
							new TreeSet<Object>((Collection<Object>)secondaryIndexKeys));							
					}
					
					for (SecondaryIndexIdentifiable secondaryIndexInstance : secondaryIndexInstancesFromHiveScenario) {						
						Node nodeOfSecondaryIndexKey = hive.getNodeOfSecondaryIndexKey(
								partitionDimension.getName(),
								resource.getName(),
								secondaryIndex.getName(),
								secondaryIndexInstance.getIdAsSecondaryIndexInstance());
						assertNotNull(nodeOfSecondaryIndexKey);
						
						// Assert that querying for the primary key of the secondary index key yields what we expect
						Object expectedPrimaryIndexKey = secondaryIndexInstance.getPrimaryIndexIdAsSecondaryIndexInstance();
						Object actualPrimaryIndexKey = hive.getPrimaryIndexKeyOfSecondaryIndexKey(
								partitionDimension.getName(),
								resource.getName(),
								secondaryIndex.getName(),
								secondaryIndexInstance.getIdAsSecondaryIndexInstance());
						assertEquals(expectedPrimaryIndexKey, actualPrimaryIndexKey);
						
						// Assert that the node of the secondary index key is the same as that of the primary index key
						Node nodeOfPrimaryIndexKey = hive.getNodeOfPrimaryIndexKey(
																					partitionDimension.getName(),
																					actualPrimaryIndexKey);
						assertEquals(nodeOfSecondaryIndexKey, nodeOfPrimaryIndexKey);						
					}
				}			
			}
		}
	}

	private static Collection<SecondaryIndexIdentifiable> filterSecondaryIndexInstancesOfPrimaryIndexInstance(Collection<SecondaryIndexIdentifiable> secondaryIndexInstancesFromHiveScenario, final PrimaryIndexIdentifiable primaryIndexInstance) {
		return Filter.grep(new Predicate<SecondaryIndexIdentifiable>() {
			public boolean f(SecondaryIndexIdentifiable secondaryIndexInstance) {
				return secondaryIndexInstance.getPrimaryIndexIdAsSecondaryIndexInstance() == primaryIndexInstance.getIdAsPrimaryIndexInstance();
			}
		}, secondaryIndexInstancesFromHiveScenario);
	}

	public void validateUpdatesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		final Hive hive = hiveScenario.getHive();
		for (final PartitionDimension partitionDimension : hiveScenario.getCreatedPartitionDimensions()) {
			updatePimaryIndexIdentifiables(hive, hiveScenario, partitionDimension, new Filter.AllAllFilter());
			updateSecondaryIndexIdentifiables(hive, hiveScenario, partitionDimension, new Filter.AllAllFilter());			
		}
		PartitionDimension anyPartitionDimension = null;
		try {
			anyPartitionDimension = Atom.getFirst(hiveScenario.getCreatedPartitionDimensions());
		} catch (Exception e) { throw new RuntimeException(e); } 
		updateMetaData(hiveScenario, hive, anyPartitionDimension);
		commitReadonlyViolations(hiveScenario, hive, anyPartitionDimension);
	}

	private static void updatePimaryIndexIdentifiables(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			new Undoable() {
				public void f() throws Exception {
					
					final Map<Node,Node> nodeToNodeMap = makeThisToThatMap( partitionDimension.getNodeGroup().getNodes());
					//	Update the node of each primary key to another node, according to this map
					for (final PrimaryIndexIdentifiable primaryIndexInstance : iterateFilter.f(hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension))) {								
						final Node originalNode = hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance());
						final boolean readOnly = hive.getReadOnlyOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance());
						updateNode(primaryIndexInstance, originalNode, nodeToNodeMap);							
						updateReadOnly(primaryIndexInstance, !readOnly);
						
						new Undo() { public void f() throws Exception {							
								final Map<Node,Node> reverseNodeToNodeMap = Transform.reverseMap(nodeToNodeMap);
								Node newNode = hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance());
								updateReadOnly(primaryIndexInstance, readOnly);
								updateNode(primaryIndexInstance, newNode, reverseNodeToNodeMap);									
						}};
					}
				}
				private void updateNode(PrimaryIndexIdentifiable primaryIndexInstance, Node fromNode, Map<Node,Node> mapToNewNode) throws Exception {
					hive.updatePrimaryIndexNode(
							partitionDimension,
							primaryIndexInstance.getIdAsPrimaryIndexInstance(),
							mapToNewNode.get(fromNode)); // note, using Node instead of node uri since our tests use duplicate node uris
					assertEquals(mapToNewNode.get(fromNode).getId(), hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance()).getId());
				}
				private void updateReadOnly(PrimaryIndexIdentifiable primaryIndexInstance, boolean toBool) throws HiveException, SQLException {
					hive.updatePrimaryIndexReadOnly(
							partitionDimension.getName(),
							primaryIndexInstance.getIdAsPrimaryIndexInstance(),
							toBool);
					assertEquals(toBool, hive.getReadOnlyOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance()));
				}
			}.cycle();
		} catch (Exception e)  { throw new HiveException("Undoable exception", e); }
	}
	private static void updateSecondaryIndexIdentifiables(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			new Undoable() {
				public void f() throws Exception {
					final Map<Object,Object> primaryIndexKeyToPrimaryIndexKeyMap = makeThisToThatMap(
							Transform.map(new Unary<PrimaryIndexIdentifiable,Object>() {
								public Object f(PrimaryIndexIdentifiable primaryIndexInstance) { return primaryIndexInstance.getIdAsPrimaryIndexInstance(); }},
								hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension)));
					
					
					final Map<Object,Object> reversePrimaryIndexKeyToPrimaryIndexKeyMap = Transform.reverseMap(primaryIndexKeyToPrimaryIndexKeyMap);
					
					// Validate that the secondary index keys are in the database with the right primary index key
					for (final Resource resource : iterateFilter.f(partitionDimension.getResources())) {
						
						for (final SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {	
							Collection<SecondaryIndexIdentifiable> secondaryIndexInstancesFromHiveScenario = hiveScenario.getSecondaryIndexInstancesForThisPartitionDimensionAndResource(partitionDimension, resource, secondaryIndex);							
							//	Update each primary index key of the secondary index keys
							for (final SecondaryIndexIdentifiable secondaryIndexKeyInstance : secondaryIndexInstancesFromHiveScenario) {																			
								final Object newPrimaryIndexKey = primaryIndexKeyToPrimaryIndexKeyMap.get(secondaryIndexKeyInstance.getPrimaryIndexIdAsSecondaryIndexInstance());
								updatePrimaryKeyOfSecondaryKey(hive, partitionDimension, resource, secondaryIndex, secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance(), newPrimaryIndexKey);																							
								new Undo() { public void f() throws Exception {						
									updatePrimaryKeyOfSecondaryKey(hive, partitionDimension, resource, secondaryIndex, secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance(), reversePrimaryIndexKeyToPrimaryIndexKeyMap.get(newPrimaryIndexKey));											
								}};
							}
						}
					}
				}
				private void updatePrimaryKeyOfSecondaryKey(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object primaryIndexKey) throws HiveException, SQLException {
					hive.updatePrimaryIndexKeyOfSecondaryIndexKey(
							partitionDimension.getName(),
							resource.getName(),
							secondaryIndex.getName(),
							secondaryIndexKey,
							primaryIndexKey);
					
					// reload each secondary index key and assert it has its new primary index key
					assertEquals(primaryIndexKey,
								 hive.getPrimaryIndexKeyOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey));								
				}
			}.cycle();
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}
	
	private static void updateMetaData(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimensionFromHiveScenario)
	{
		try {
			final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionFromHiveScenario.getName());
			new Undoable() {
				public void f() throws Exception {						
					final String name = partitionDimension.getName();			
					final Assigner assigner = partitionDimension.getAssigner();
					final int columnType = partitionDimension.getColumnType();
					final String indexUri = partitionDimension.getIndexUri();
					partitionDimension.setAssigner(new Assigner() {
						public Node chooseNode(Collection<Node> nodes, Object value) {
							return null;
						}				
					});
					partitionDimension.setColumnType(JdbcTypeMapper.parseJdbcType(JdbcTypeMapper.FLOAT));
					partitionDimension.setIndexUri("jdbc:mysql://arb/it?user=ra&password=ry");
					hive.updatePartitionDimension(partitionDimension);			
					assertEquality(hive, partitionDimension);
					
					new Undo() {							
						public void f() throws Exception {
							partitionDimension.setName(name);
							partitionDimension.setColumnType(columnType);
							partitionDimension.setAssigner(assigner);
							partitionDimension.setIndexUri(indexUri);
							hive.updatePartitionDimension(partitionDimension);			
							assertEquality(hive, partitionDimension);
						}
					};
				}

				private void assertEquality(final Hive hive, final PartitionDimension partitionDimension) throws HiveException {
					assertEquals(
						partitionDimension,
						hive.getPartitionDimension(partitionDimension.getName()));
					AssertUtils.assertThrows(new AssertUtils.UndoableToss() {  public void f() throws Exception {
						final String name = partitionDimension.getName();
						new Undo() { public void f() throws Exception {								
							partitionDimension.setName(name);	
						}};
						// Verify that the name can't match another partition dimension name
						partitionDimension.setName(Atom.getFirst(Atom.getRest((hive.getPartitionDimensions()))).getName());
						hive.updatePartitionDimension(partitionDimension);	
					}});
				}
			}.cycle();
	
			new Undoable() { 
				public void f() throws Exception {
					final Node node = Atom.getFirst(partitionDimension.getNodeGroup().getNodes());
					final boolean readOnly = node.isReadOnly();
					final String uri = node.getUri();
					node.setReadOnly(!readOnly);
					node.setUri("jdbc:mysql://arb/it?user=ra&password=ry");
					hive.updateNode(node);			
					assertEquality(hive, node);
					new Undo() {							
						public void f() throws Exception {
							node.setReadOnly(readOnly);
							node.setUri(uri);
							hive.updateNode(node);			
							assertEquality(hive, node);
						}
					};
				}
				private void assertEquality(final Hive hive, final Node node) throws HiveException {
					assertEquals(
						node,
						hive.getPartitionDimension(node.getNodeGroup().getPartitionDimension().getName()).getNodeGroup().getNode(node.getId()));
				}
			}.cycle();
			
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			new Undoable() { 
				public void f() throws Exception {			
		
					final String name = resource.getName();		
					resource.setName("X");
					hive.updateResource(resource);		
					assertEquality(hive, partitionDimension, resource);
					
					new Undo() {							
						public void f() throws Exception {
							resource.setName(name);			
							hive.updateResource(resource);		
							assertEquality(hive, partitionDimension, resource);
						}
					};
				}

				private void assertEquality(final Hive hive, final PartitionDimension partitionDimension, final Resource resource) throws HiveException {
					assertEquals(
						resource,
						hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()));
				}
			}.cycle();
			
			new Undoable() { 
				public void f() throws Exception {			
					final SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());
				
					final ColumnInfo columnInfo = secondaryIndex.getColumnInfo();	
					secondaryIndex.setColumnInfo(new ColumnInfo("X", JdbcTypeMapper.parseJdbcType(JdbcTypeMapper.FLOAT)));
					hive.updateSecondaryIndex(secondaryIndex);
					assertEquality(hive, partitionDimension, resource, secondaryIndex);
					
					new Undo() {							
						public void f() throws Exception {
							secondaryIndex.setColumnInfo(columnInfo);
							hive.updateSecondaryIndex(secondaryIndex);
							assertEquality(hive, partitionDimension, resource, secondaryIndex);
						}
					};
				}
				private void assertEquality(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final SecondaryIndex secondaryIndex) throws HiveException {
					assertEquals(
						secondaryIndex,
						hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()).getSecondaryIndex(secondaryIndex.getName()));
				}
			}.cycle();
		}
		catch (Exception e) { throw new RuntimeException("Undoable exception", e); }
										
	}
	private static void commitReadonlyViolations(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimensionFromHiveScenario) throws HiveException 
	{
		try {
			// get some sample instances
			final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionFromHiveScenario.getName());
			final PrimaryIndexIdentifiable primaryIndexInstance = Atom.getFirst(hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension));
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			final SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());
			final SecondaryIndexIdentifiable secondaryIndexInstance = Atom.getFirst(hiveScenario.getSecondaryIndexInstancesForThisPartitionDimensionAndResource(partitionDimension, resource, secondaryIndex));
			
			final Node node = hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance());
			
			
			// Test update the hive to reaonly and then try to make various updates
			
			// 	Attempt to update a primary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.updatePrimaryIndexNode(partitionDimension,	primaryIndexInstance.getIdAsPrimaryIndexInstance(),	node); // expect throw, so node doesn't matter															
			}}, HiveReadOnlyException.class);	
			
			// Attempt to insert a secondary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.setReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.insertSecondaryIndexKey(secondaryIndex,
					InstallHiveIndexSchema.constructSecondaryInstance(primaryIndexInstance.getClass(), secondaryIndexInstance.getClass()),
					primaryIndexInstance.getIdAsPrimaryIndexInstance());
			}}, HiveReadOnlyException.class);	
			
			// Test update a node to readonly and then try to update a partition index key that is on that node
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				node.setReadOnly(true);
				hive.updateNode(node);
				new Undo() { public void f() throws Exception {
					node.setReadOnly(false);
					hive.updateNode(node);
				}};
				hive.updatePrimaryIndexNode(
						partitionDimension,
						primaryIndexInstance.getIdAsPrimaryIndexInstance(),
						node); // expect throw, so node doesn't matter															
			}}, HiveReadOnlyException.class);					
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}	
	
	public void validateDeletesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		final Hive hive = hiveScenario.getHive();
		for (final PartitionDimension partitionDimension : hiveScenario.getCreatedPartitionDimensions()) {					
			// Validate that the secondary index keys are in the database with the right primary index key
			for (final Resource resource : partitionDimension.getResources()) {
				
				for (final SecondaryIndex secondaryIndex : resource.getSecondaryIndexes()) {	
					Collection<SecondaryIndexIdentifiable> secondaryIndexInstancesFromHiveScenario = hiveScenario.getSecondaryIndexInstancesForThisPartitionDimensionAndResource(partitionDimension, resource, secondaryIndex);
					
					//	Delete each secondary index key
					for (final SecondaryIndexIdentifiable secondaryIndexKeyInstance : secondaryIndexInstancesFromHiveScenario) {							
						try {
							new Undoable() { public void f() throws Exception {
								
								hive.deleteSecondaryIndexKey(
									partitionDimension.getName(),
									resource.getName(),
									secondaryIndex.getName(),
									secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance());
								assertFalse(hive.doesSecondaryIndexKeyExist( partitionDimension.getName(), resource.getName(), secondaryIndex.getName(), secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance()));
				
								new Undo() { public void f() throws Exception {
									hive.insertSecondaryIndexKey(
										partitionDimension.getName(),
										resource.getName(),
										secondaryIndex.getName(),
										secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance(),
										secondaryIndexKeyInstance.getPrimaryIndexIdAsSecondaryIndexInstance()
									);
									assertEquals(secondaryIndexKeyInstance.getPrimaryIndexIdAsSecondaryIndexInstance(),
												hive.getPrimaryIndexKeyOfSecondaryIndexKey( partitionDimension.getName(), resource.getName(), secondaryIndex.getName(), secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance()));
								}};
							}}.cycle(); 
						} catch(Exception e) { throw new HiveException("Undoable exception", e); }
					}			
				}			
			}
			try {
				new Undoable() { public void f() throws Exception {
					for (final PrimaryIndexIdentifiable primaryIndexInstance : hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension)) {
						hive.deletePrimaryIndexKey(partitionDimension.getName(), primaryIndexInstance.getIdAsPrimaryIndexInstance());
						assertFalse(hive.doesPrimaryIndeyKeyExist(partitionDimension.getName(), primaryIndexInstance.getIdAsPrimaryIndexInstance()));
						
						for (final Resource resource : partitionDimension.getResources())					
							for (final SecondaryIndex secondaryIndex : resource.getSecondaryIndexes())	{
							
									final Collection<SecondaryIndexIdentifiable> secondaryIndexInstancesFromHiveScenario = hiveScenario.getSecondaryIndexInstancesForThisPartitionDimensionAndResource(partitionDimension, resource, secondaryIndex);						
									assertEquals(0, hive.getSecondaryIndexKeysWithPrimaryKey(secondaryIndex, primaryIndexInstance.getIdAsPrimaryIndexInstance()).size());
									new Undo() { public void f() throws Exception {
										for (final SecondaryIndexIdentifiable secondaryIndexKeyInstance : filterSecondaryIndexInstancesOfPrimaryIndexInstance(secondaryIndexInstancesFromHiveScenario, primaryIndexInstance)) 																					
											hive.insertSecondaryIndexKey(secondaryIndex, secondaryIndexKeyInstance.getIdAsSecondaryIndexInstance(), primaryIndexInstance.getIdAsPrimaryIndexInstance());										
									}};								
							}
						
						new Undo() { public void f() throws Exception {				
							hive.insertPrimaryIndexKey(partitionDimension, primaryIndexInstance.getIdAsPrimaryIndexInstance());
							assertTrue(hive.doesPrimaryIndeyKeyExist(partitionDimension.getName(), primaryIndexInstance.getIdAsPrimaryIndexInstance()));
						}};
					}
				}}.cycle();
			} catch (Exception e) { throw new HiveException("Undoable exception", e); }
		}
	
	}
	
	private static<T> Map<T,T> makeThisToThatMap(Collection<T> items) {
		
		// Update the node of each primary index key to the node given by this map
		RingIteratorable<T> iterator = new RingIteratorable<T>(items, items.size()+1);
		final Queue<T> queue = new LinkedList<T>();
		queue.add(iterator.next());
		return Transform.toMap(
			new Transform.IdentityFunction<T>(), 
			new Unary<T,T>() {
				public T f(T item) {
					queue.add(item);
					return queue.remove();							
				}
			},
			iterator);
	}


}
