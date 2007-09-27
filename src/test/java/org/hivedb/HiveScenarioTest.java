package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.ColumnInfo;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.PrimaryIndexIdentifiableGeneratableImpl;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.ResourceIdentifiableGeneratableImpl;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.ExceptionalActor;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Undoable;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;
import org.hivedb.util.scenarioBuilder.PartitionDimensionCreator;

public class HiveScenarioTest {
	
	HiveScenarioConfig hiveScenarioConfig;
	Collection<Node> dataNodes;
	private NodeResolver directory;
	public HiveScenarioTest(HiveScenarioConfig hiveScenarioConfig)
	{
		this.hiveScenarioConfig = hiveScenarioConfig;
	}
	public void performTest(int primaryIndexInstanceCount, int resourceInstanceCount) {
		HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, primaryIndexInstanceCount, resourceInstanceCount);
		validate(hiveScenario);
	}

	protected void validate(HiveScenario hiveScenario) {
		try {
			validateHiveMetadata(hiveScenario);
			// Validate CRUD operations. Read at the beginning and after updates
			// to verify that the update restored the data to its original state.
			validateReadsFromPersistence(hiveScenario);
			validateUpdatesToPersistence(hiveScenario);
			validateReadsFromPersistence(hiveScenario);
			validateDeletesToPersistence(hiveScenario);
			// data is reinserted after deletes but nodes can change so we can't validate equality again
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void validateHiveMetadata(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHiveScenarioConfig().getHive();
		String partitionDimensionName = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName();
		PartitionDimension expectedPartitionDimension = PartitionDimensionCreator.create(hiveScenarioConfig);
		PartitionDimension actualPartitionDimension = hive.getPartitionDimension(partitionDimensionName);
		
		// Validate our PartitionDimension in memory against those that are in the persistence
		// This tests all the hive metadata.
		assertEquals(String.format("Expected %s but got %s", hive.getPartitionDimension(partitionDimensionName), hive.getPartitionDimensions()),			
			actualPartitionDimension,
			expectedPartitionDimension);
	}
	
	@SuppressWarnings("unchecked")
	public void validateReadsFromPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHiveScenarioConfig().getHive();
		String partitionDimensionName = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName();
		PartitionDimension expectedPartitionDimension = PartitionDimensionCreator.create(hiveScenarioConfig);
		final PartitionDimension actualPartitionDimension = hive.getPartitionDimension(partitionDimensionName);
		
		directory = new Directory(actualPartitionDimension,new HiveBasicDataSource(hive.getUri()));
		for (Object primaryindexKey : hiveScenario.getGeneratedPrimaryIndexKeys())
			assertTrue(directory.getNodeSemamphoresOfPrimaryIndexKey(primaryindexKey).size() > 0);
		
		assertEquals(
					new TreeSet<Resource>(expectedPartitionDimension.getResources()),
					new TreeSet<Resource>(actualPartitionDimension.getResources()));
		
		// Validate that the secondary index keys are in the database with the right primary index key
		for (final Resource resource : actualPartitionDimension.getResources()) {
			
			Collection<Object> resourceInstances = hiveScenario.getGeneratedResourceInstances();
			for (final Object resourceInstance : resourceInstances) {
				ResourceIdentifiable<Object> resourceIdentifiable = hiveScenarioConfig.getResourceIdentifiable();
				final PrimaryIndexIdentifiable primaryIndexIdentifiable = resourceIdentifiable.getPrimaryIndexIdentifiable();
									
				Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = resourceIdentifiable.getSecondaryIndexIdentifiables();
				for (SecondaryIndexIdentifiable secondaryIndexIdentifiable : secondaryIndexIdentifiables) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName());
					
					//  Assert that querying for all the secondary index keys of a primary index key returns the right collection
					final List<Object> secondaryIndexKeys = new ArrayList<Object>(hive.getSecondaryIndexKeysWithPrimaryKey(
																secondaryIndex.getName(),
																resource.getName(),
																actualPartitionDimension.getName(),
																primaryIndexIdentifiable.getPrimaryIndexKey(resourceInstance)));
					
					Object secondaryIndexValue = secondaryIndexIdentifiable.getSecondaryIndexValue(resourceInstance);
					new Actor<Object>(secondaryIndexValue) {
						public void f(Object secondaryIndexKey) {
							assertTrue(String.format("directory.getSecondaryIndexKeysWithPrimaryKey(%s,%s,%s,%s)", secondaryIndex.getName(), resource.getName(), actualPartitionDimension.getName(), secondaryIndexKey),
									secondaryIndexKeys.contains(secondaryIndexKey));
					
							Collection<KeySemaphore> nodeSemaphoreOfSecondaryIndexKeys = directory.getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
							assertTrue(String.format("directory.getNodeSemaphoresOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), secondaryIndexKey),
									   nodeSemaphoreOfSecondaryIndexKeys.size() > 0);
								
							// Assert that querying for the primary key of the secondary index key yields what we expect
							Object expectedPrimaryIndexKey = primaryIndexIdentifiable.getPrimaryIndexKey(resourceInstance);
							Collection<Object> actualPrimaryIndexKeys = directory.getPrimaryIndexKeysOfSecondaryIndexKey(
									secondaryIndex,
									secondaryIndexKey);
							assertTrue(String.format("directory.getPrimaryIndexKeysOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), secondaryIndexKey),
									Filter.grepItemAgainstList(expectedPrimaryIndexKey, actualPrimaryIndexKeys));
						
							// Assert that one of the nodes of the secondary index key is the same as that of the primary index key
							// There are multiple nodes returned when multiple primray index keys exist for a secondary index key
							Collection<KeySemaphore> nodeSemaphoreOfPrimaryIndexKey = directory.getNodeSemamphoresOfPrimaryIndexKey(expectedPrimaryIndexKey);
							for(KeySemaphore semaphore : nodeSemaphoreOfPrimaryIndexKey)
								assertTrue(Filter.grepItemAgainstList(semaphore, nodeSemaphoreOfSecondaryIndexKeys));	
					}}.perform();			
				}
			}	
		}
	}

	public void validateUpdatesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHiveScenarioConfig().getHive();
		String partitionDimensionName = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionName);
		updatePimaryIndexKeys(hive, hiveScenario, partitionDimension, new Filter.AllowAllFilter());
		updateResourceAndSecondaryIndexKeys(hive, hiveScenario, partitionDimension, new Filter.AllowAllFilter());			
		updateMetaData(hiveScenario, hive, partitionDimension);
		commitReadonlyViolations(hiveScenario, hive, partitionDimension);
	}

	private static void updatePimaryIndexKeys(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			Undoable undoable = new Undoable() {
				public void f() {
					//	Update the node of each primary key to another node, according to this map
					for (final Object primaryIndexKey : iterateFilter.f(hiveScenario.getGeneratedPrimaryIndexKeys())) {								
						final boolean readOnly = hive.getReadOnlyOfPrimaryIndexKey(partitionDimension.getName(), primaryIndexKey);						
						try {
							updateReadOnly(primaryIndexKey, !readOnly);
						} catch (Exception e) { throw new RuntimeException(e); }
						new Undo() { public void f() {							
							try {
								updateReadOnly(primaryIndexKey, readOnly);
							} catch (Exception e) { throw new RuntimeException(e); }
						}};
					}
				}
				
				private void updateReadOnly(Object primaryIndexKey, boolean toBool) throws HiveException, SQLException {
					hive.updatePrimaryIndexReadOnly(
							partitionDimension.getName(),
							primaryIndexKey,
							toBool);
					assertEquals(toBool, hive.getReadOnlyOfPrimaryIndexKey(partitionDimension.getName(), primaryIndexKey));
				}
			};
			undoable.cycle();
		} catch (Exception e)  { throw new HiveException("Undoable exception", e); }
	}
	
	private static void updateResourceAndSecondaryIndexKeys(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			new Undoable() {
				public void f() {
					
					final Map<Object,Object> primaryIndexKeyToPrimaryIndexKeyMap = makeThisToThatMap(hiveScenario.getGeneratedPrimaryIndexKeys());																
					final Map<Object,Object> reversePrimaryIndexKeyToPrimaryIndexKeyMap = Transform.reverseMap(primaryIndexKeyToPrimaryIndexKeyMap);
								
					final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable();
					final PrimaryIndexIdentifiable primaryIndexIdentifiable = resourceIdentifiable.getPrimaryIndexIdentifiable();
					for (final Resource resource : iterateFilter.f(partitionDimension.getResources())) {
						
						for (final Object resourceInstance : hiveScenario.getGeneratedResourceInstances()) {							
							
							final Object primaryIndexKey = primaryIndexIdentifiable.getPrimaryIndexKey(resourceInstance);
							final Object newPrimaryIndexKey = primaryIndexKeyToPrimaryIndexKeyMap.get(primaryIndexKey);
							try {
								updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, resourceIdentifiable.getId(resourceInstance), newPrimaryIndexKey, primaryIndexKey);																							
							} catch (Exception e) { throw new RuntimeException(e); }
							new Undo() { public void f() {						
								try {
									updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, resourceIdentifiable.getId(resourceInstance), reversePrimaryIndexKeyToPrimaryIndexKeyMap.get(newPrimaryIndexKey), newPrimaryIndexKey);											
								} catch (Exception e) { throw new RuntimeException(e); }
							}};	
						}
					}
				}
				private void updatePrimaryIndexKeyOfResourceId(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final Object resourceId, final Object newPrimaryIndexKey, final Object originalPrimaryIndexKey) throws HiveException, SQLException {
					hive.updatePrimaryIndexKeyOfResourceId(
							partitionDimension.getName(),
							resource.getName(),
							resourceId,
							originalPrimaryIndexKey,
							newPrimaryIndexKey);
						
					assertEquals(newPrimaryIndexKey, Atom.getFirstOrThrow(hive.getPrimaryIndexKeysOfSecondaryIndexKey(resource.getIdIndex(), resourceId)));								
				}
			}.cycle();
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}
	
	private static void updateMetaData(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimensionFromHiveScenario)
	{
		try {
			final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionFromHiveScenario.getName());
			new Undoable() {
				public void f() {						
					final String name = partitionDimension.getName();			
					final Assigner assigner = partitionDimension.getAssigner();
					final int columnType = partitionDimension.getColumnType();
					final String indexUri = partitionDimension.getIndexUri();
					partitionDimension.setAssigner(new Assigner() {
						public Node chooseNode(Collection<Node> nodes, Object value) {
							return null;
						}

						public Collection<Node> chooseNodes(Collection<Node> nodes, Object value) {
							return Arrays.asList(new Node[]{chooseNode(nodes,value)});
						}				
					});
					partitionDimension.setColumnType(JdbcTypeMapper.parseJdbcType(JdbcTypeMapper.FLOAT));
					partitionDimension.setIndexUri("jdbc:mysql://arb/it?user=ra&password=ry");
					try {
						hive.updatePartitionDimension(partitionDimension);
					} catch (Exception e) { throw new RuntimeException(e); }
						
					assertEquality(hive, partitionDimension);
					
					new Undo() {							
						public void f() {
							partitionDimension.setName(name);
							partitionDimension.setColumnType(columnType);
							partitionDimension.setAssigner(assigner);
							partitionDimension.setIndexUri(indexUri);
							try {
								hive.updatePartitionDimension(partitionDimension);
							} catch (Exception e) { throw new RuntimeException(e); }
							assertEquality(hive, partitionDimension);
						}
					};
				}

			
			}.cycle();
	
			new Undoable() { 
				public void f() {
					final Node node = Atom.getFirstOrThrow(partitionDimension.getNodes());
					final boolean readOnly = node.isReadOnly();
					final String uri = node.getUri();
					node.setReadOnly(!readOnly);
					node.setUri("jdbc:mysql://arb/it?user=ra&password=ry");
					try {
						hive.updateNode(node);			
					} catch (Exception e) { throw new RuntimeException(e); }
					assertEquality(hive, node);
					new Undo() {							
						public void f() {
							node.setReadOnly(readOnly);
							node.setUri(uri);
							try {
								hive.updateNode(node);
							} catch (Exception e) { throw new RuntimeException(e); }
							assertEquality(hive, node);
						}
					};
				}
				
			}.cycle();
			
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			new Undoable() { 
				public void f() {			
		
					final String name = resource.getName();		
					resource.setName("X");
					try {
						hive.updateResource(resource);
					} catch (Exception e) { throw new RuntimeException(e); }
					assertEquality(hive, partitionDimension, resource);
					
					new Undo() {							
						public void f() {
							resource.setName(name);			
							try {
								hive.updateResource(resource);
							} catch (Exception e) { throw new RuntimeException(e); }
							assertEquality(hive, partitionDimension, resource);
						}
					};
				}

				
			}.cycle();
			
			new Undoable() { 
				public void f() {			
					final SecondaryIndex secondaryIndex = Atom.getFirstOrThrow(resource.getSecondaryIndexes());
				
					final ColumnInfo columnInfo = secondaryIndex.getColumnInfo();	
					secondaryIndex.setColumnInfo(new ColumnInfo("X", JdbcTypeMapper.parseJdbcType(JdbcTypeMapper.FLOAT)));
					try {
						hive.updateSecondaryIndex(secondaryIndex);
					} catch (Exception e) { throw new RuntimeException(e); }
					
					assertEquality(hive, partitionDimension, resource, secondaryIndex);
					
					new Undo() {							
						public void f() {
							secondaryIndex.setColumnInfo(columnInfo);
							try {
								hive.updateSecondaryIndex(secondaryIndex);
							} catch (Exception e) { throw new RuntimeException(e); }
							assertEquality(hive, partitionDimension, resource, secondaryIndex);
						}
					};
				}
				
			}.cycle();
		}
		catch (Exception e) { throw new RuntimeException("Undoable exception", e); }
										
	}
	
	private static void assertEquality(final Hive hive, final PartitionDimension partitionDimension) {
		assertEquals(
			partitionDimension,
			hive.getPartitionDimension(partitionDimension.getName()));
		AssertUtils.assertThrows(new AssertUtils.UndoableToss() {  public void f() throws Exception {
			final String name = partitionDimension.getName();
			new Undo() { public void f() throws Exception {								
				partitionDimension.setName(name);	
			}};
			// Verify that the name can't match another partition dimension name
			partitionDimension.setName(Atom.getFirstOrThrow(Atom.getRestOrThrow((hive.getPartitionDimensions()))).getName());
			hive.updatePartitionDimension(partitionDimension);
		}});
	}
	private static void assertEquality(final Hive hive, final PartitionDimension partitionDimension, final Resource resource) {
		assertEquals(
			resource,
			hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()));
	}
	private static void assertEquality(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final SecondaryIndex secondaryIndex)  {
		assertEquals(
			secondaryIndex,
			hive.getPartitionDimension(partitionDimension.getName()).getResource(resource.getName()).getSecondaryIndex(secondaryIndex.getName()));
	}
	private static void assertEquality(final Hive hive, final Node node)  {
		assertEquals(
			node,
			hive.getPartitionDimension(node.getPartitionDimensionId()).getNode(node.getId()));
	}
	
	private static void commitReadonlyViolations(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimensionFromHiveScenario) throws HiveException 
	{
		try {
			// get some sample instances
			final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionFromHiveScenario.getName());
			final Object primaryIndexKey = Atom.getFirst(hiveScenario.getGeneratedPrimaryIndexKeys());
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			final SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());			
			final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable();
			
			// Attempt to insert a secondary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				Object newResourceInstance = new ResourceIdentifiableGeneratableImpl<Object>(resourceIdentifiable)
										.generate(primaryIndexKey);
				Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = 
					resourceIdentifiable .getSecondaryIndexIdentifiables();
				final Object secondaryIndexValue = Atom.getFirst(secondaryIndexIdentifiables).getSecondaryIndexValue(newResourceInstance);
				new ExceptionalActor<Object,HiveReadOnlyException>(secondaryIndexValue) {
					public void f(Object secondaryIndexKey) throws HiveReadOnlyException {
						hive.insertSecondaryIndexKey(
								secondaryIndex.getName(), 
								resource.getName(), 
								secondaryIndex.getResource().getPartitionDimension().getName(),
								secondaryIndexKey,
								primaryIndexKey);
				}}.perform();
			}}, HiveReadOnlyException.class);	
			
			// Attempt to insert a primary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.insertPrimaryIndexKey(partitionDimension.getName(), new PrimaryIndexIdentifiableGeneratableImpl(resourceIdentifiable).f());
			}}, HiveReadOnlyException.class);	
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}	
	
	public void validateDeletesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHiveScenarioConfig().getHive();
		String partitionDimensionName = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable().getPrimaryIndexIdentifiable().getPartitionDimensionName();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(partitionDimensionName);					
		validateDeletePrimaryIndexKey(hiveScenario, hive, partitionDimension);	
		validateDeleteResourceIdentifiable(hiveScenario, hive, partitionDimension);
		validateDeleteSecondaryIndexIdentifiable(hiveScenario, hive, partitionDimension);
	}
	
	
	private void validateDeletePrimaryIndexKey(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable();
		for (final Object primaryIndexKey : hiveScenario.getGeneratedPrimaryIndexKeys()) {
			for (final Resource resource : partitionDimension.getResources()) {						
				new Undoable() { public void f() {
					try {
						hive.deletePrimaryIndexKey(partitionDimension.getName(), primaryIndexKey);
					} catch (Exception e) { throw new RuntimeException(e); }
					assertFalse(hive.doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey));
				
					for (final Object resourceInstance :
							Filter.grep(new Predicate<Object>() {
								public boolean f(Object resourceInstance) {
									return resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey(resourceInstance).equals(primaryIndexKey);
								}},
								hiveScenario.getGeneratedResourceInstances())) {	
						
						assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId(resourceInstance)));
					
						Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = resourceIdentifiable.getSecondaryIndexIdentifiables();
						for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : secondaryIndexIdentifiables) {	
						
							new Undo() { public void f()  {
								undoSecondaryIndexDelete(
										hiveScenarioConfig,
										secondaryIndexIdentifiable,
										resourceInstance);
							}};								
						}
						
						new Undo() { public void f() {
							try {
								hive.insertResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId(resourceInstance), resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey(resourceInstance));
							} catch (Exception e) { throw new RuntimeException(e); }
							assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId(resourceInstance)));
						}};
					}
					
					new Undo() { public void f()  {				
						try {
							hive.insertPrimaryIndexKey(partitionDimension.getName(), primaryIndexKey);
						} catch (Exception e) { throw new RuntimeException(e); }
						assertTrue(hive.doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey));
					}};
				}}.cycle();					
			}
		}
	}
	private void validateDeleteResourceIdentifiable(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable();
		for (final Resource resource : partitionDimension.getResources()) {
			Collection<Object> resourceInstances = hiveScenario.getGeneratedResourceInstances();
			for (final Object resourceInstance : resourceInstances) {
			
				// Test delete of a resource id and the cascade delete of its secondary index key
				try {
					new Undoable() {
						public void f() {
							try {
								hive.deleteResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId(resourceInstance));
							} catch (Exception e) { throw new RuntimeException(e); }
							assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId(resourceInstance)));
							
							Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = resourceIdentifiable.getSecondaryIndexIdentifiables();
							for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : secondaryIndexIdentifiables) {	
								final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName());
								Object secondaryIndexValue = secondaryIndexIdentifiable.getSecondaryIndexValue(resourceInstance);
								new Actor(secondaryIndexValue) {
									public void f(Object secondaryIndexKey) throws Exception {
										assertFalse(Filter.grepItemAgainstList(
												resourceIdentifiable.getId(resourceInstance),
												hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey)));
									}
								};
								
							
								new Undo() { public void f()  {
									try {
										undoSecondaryIndexDelete(hiveScenarioConfig, secondaryIndexIdentifiable, resourceInstance);
									} catch (Exception e) { throw new RuntimeException(e); }
								}};
							}
							
							new Undo() { public void f() {
								try {
									hive.insertResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId(resourceInstance), resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey(resourceInstance));
								} catch (Exception e) { throw new RuntimeException(e); }
								assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId(resourceInstance)));
							}};
					}}.cycle();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	private void validateDeleteSecondaryIndexIdentifiable(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		final ResourceIdentifiable<Object> resourceIdentifiable = hiveScenario.getHiveScenarioConfig().getResourceIdentifiable();
		for (final Resource resource : partitionDimension.getResources()) {
			Collection<Object> resourceInstances = hiveScenario.getGeneratedResourceInstances();
			for (final Object resourceInstance : resourceInstances) {
				// Test delete of secondary index keys individually
				Collection<? extends SecondaryIndexIdentifiable> secondaryIndexIdentifiables = resourceIdentifiable.getSecondaryIndexIdentifiables();
				for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : secondaryIndexIdentifiables) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName());
					
					new Undoable() { 
						public void f() {
							final Object resourceId = resourceIdentifiable.getId(resourceInstance);
							final Object secondaryIndexValue = secondaryIndexIdentifiable.getSecondaryIndexValue(resourceInstance);
							new Actor<Object>(secondaryIndexValue) {
								public void f(final Object secondaryIndexKey) throws RuntimeException {
									try {
										
										hive.deleteSecondaryIndexKey(
											secondaryIndex.getName(),
											resource.getName(),
											partitionDimension.getName(),
											secondaryIndexKey,
											resourceId);
										assertFalse(Filter.grepItemAgainstList(resourceId,
													hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey)));
									} catch (Exception e) { throw new RuntimeException(e); }
									new Undo() { public void f() {
										try {
											hive.insertSecondaryIndexKey(
													secondaryIndex.getName(),
													resource.getName(),
													partitionDimension.getName(),
													secondaryIndexKey,
													resourceId
											);
										} catch (Exception e) { throw new RuntimeException(e); }
										Collection<Object> resourceIdsOfSecondaryIndexKey = hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey);
										assertTrue(Filter.grepItemAgainstList(
													resourceId, 
													resourceIdsOfSecondaryIndexKey));
									}};						
							}}.perform();
					}}.cycle(); 
				}
						
			}			
		}
	}
	
	private void undoSecondaryIndexDelete(
			final HiveScenarioConfig hiveScenarioConfig, 
			final SecondaryIndexIdentifiable secondaryIndexIdentifiable,
			final Object resourceInstance) {
		final Hive hive = hiveScenarioConfig.getHive();
		ResourceIdentifiable<Object> resourceIdentifiable = hiveScenarioConfig.getResourceIdentifiable();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(resourceIdentifiable.getPrimaryIndexIdentifiable().getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(resourceIdentifiable.getResourceName());
		final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexKeyPropertyName());																								
		final Object resourceId = resourceIdentifiable.getId(resourceInstance);
		final Object secondaryIndexValue = secondaryIndexIdentifiable.getSecondaryIndexValue(resourceInstance);
		new Actor<Object>(secondaryIndexValue) {
			public void f(Object secondaryIndexKey) {
				try {	
					hive.insertSecondaryIndexKey(
							secondaryIndex.getName(),
							resource.getName(), 
							partitionDimension.getName(),
							secondaryIndexKey, 
							resourceId);
				} catch (Exception e) { throw new RuntimeException(e); }
			assertTrue(Filter.grepItemAgainstList(
					resourceId,
					hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey)));
		}}.perform();
	};	
	
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
