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
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeSemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.Resource;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Undoable;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfig;

public class HiveScenarioTest {
	
	protected Hive hive;
	HiveScenarioConfig hiveScenarioConfig;
	private NodeResolver directory;
	public HiveScenarioTest(HiveScenarioConfig hiveScenarioConfig)
	{
		this.hiveScenarioConfig = hiveScenarioConfig;
		hive = hiveScenarioConfig.getHive();
	}
	public void performTest(int primaryIndexInstanceCount, int resourceInstanceCount) throws Exception, HiveException, SQLException {
		HiveScenario hiveScenario = HiveScenario.run(hiveScenarioConfig, primaryIndexInstanceCount, resourceInstanceCount);
		validate(hiveScenario);
	}

	protected void validate(HiveScenario hiveScenario) throws HiveException, SQLException {
		validateReadsFromPersistence(hiveScenario);
		validateUpdatesToPersistence(hiveScenario);
		validateReadsFromPersistence(hiveScenario);
		validateDeletesToPersistence(hiveScenario);
		// data is reinserted after deletes but nodes can change so we can't validate equality again
	}
	
	@SuppressWarnings("unchecked")
	public void validateReadsFromPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		Hive hive = hiveScenario.getHive();
		// Validate our PartitionDimension in memory against those that are in the persistence
		assertEquals(String.format("Expected %s but got %s", hiveScenario.getCreatedPartitionDimension(), hive.getPartitionDimensions()),
			hiveScenario.getCreatedPartitionDimension(),
			hive.getPartitionDimension(hiveScenario.getCreatedPartitionDimension().getName()));
		
		PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
		PartitionDimension partitionDimensionFromHive = hive.getPartitionDimension(partitionDimension.getName());

		directory = new Directory(partitionDimensionFromHive,new HiveBasicDataSource(hive.getHiveUri()));
		for (PrimaryIndexIdentifiable primaryIndexInstance : hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension))
			assertTrue(directory.getNodeSemamphoresOfPrimaryIndexKey(primaryIndexInstance.getPrimaryIndexKey()).size() > 0);
		
		// Validate that the hive has the created resources
		assertEquals(new TreeSet<Resource>(hiveScenario.getResourcesOfThisPartitionDimension(partitionDimension)),
					 new TreeSet<Resource>(partitionDimensionFromHive.getResources()));
		
		// Validate that the secondary index keys are in the database with the right primary index key
		for (Resource resource : partitionDimension.getResources()) {
			
			Collection<ResourceIdentifiable> resourceIdentifiables = hiveScenario.getResourceIdentifiableInstancesForThisResource(resource);
			for (ResourceIdentifiable resourceIdentifiable : resourceIdentifiables) {
				PrimaryIndexIdentifiable primaryIndexIdentifiable = resourceIdentifiable.getPrimaryIndexIdentifiable();
									
				for (SecondaryIndexIdentifiable secondaryIndexIdentifiable : resourceIdentifiable.getSecondaryIndexIdentifiables()) {	
					SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexName());
					
					//  Assert that querying for all the secondary index keys of a primary index key returns the right collection
					List secondaryIndexKeys = new ArrayList(hive.getSecondaryIndexKeysWithPrimaryKey(
																secondaryIndex.getName(),
																resource.getName(),
																partitionDimension.getName(),
																primaryIndexIdentifiable.getPrimaryIndexKey()));
					
					assertTrue(String.format("directory.getSecondaryIndexKeysWithPrimaryKey(%s,%s,%s,%s)", secondaryIndex.getName(), resource.getName(), partitionDimension.getName(), secondaryIndexIdentifiable.getSecondaryIndexKey()),
							secondaryIndexKeys.contains(secondaryIndexIdentifiable.getSecondaryIndexKey()));
			
					Collection<NodeSemaphore> nodeSemaphoreOfSecondaryIndexKeys = directory.getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, secondaryIndexIdentifiable.getSecondaryIndexKey());
					assertTrue(String.format("directory.getNodeSemaphoresOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), secondaryIndexIdentifiable.getSecondaryIndexKey()),
							   nodeSemaphoreOfSecondaryIndexKeys.size() > 0);
						
					// Assert that querying for the primary key of the secondary index key yields what we expect
					Object expectedPrimaryIndexKey = secondaryIndexIdentifiable.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPrimaryIndexKey();
					Collection<Object> actualPrimaryIndexKeys = directory.getPrimaryIndexKeysOfSecondaryIndexKey(
							secondaryIndex,
							secondaryIndexIdentifiable.getSecondaryIndexKey());
					assertTrue(String.format("directory.getPrimaryIndexKeysOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), secondaryIndexIdentifiable.getSecondaryIndexKey()),
							Filter.grepItemAgainstList(expectedPrimaryIndexKey, actualPrimaryIndexKeys));
					
					// Assert that one of the nodes of the secondary index key is the same as that of the primary index key
					// There are multiple nodes returned when multiple primray index keys exist for a secondary index key
					Collection<NodeSemaphore> nodeSemaphoreOfPrimaryIndexKey = directory.getNodeSemamphoresOfPrimaryIndexKey(expectedPrimaryIndexKey);
					for(NodeSemaphore semaphore : nodeSemaphoreOfPrimaryIndexKey)
						assertTrue(Filter.grepItemAgainstList(semaphore, nodeSemaphoreOfSecondaryIndexKeys));						
				}
			}	
		}
	}

	public void validateUpdatesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		final Hive hive = hiveScenario.getHive();
		final PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();
		updatePimaryIndexIdentifiables(hive, hiveScenario, partitionDimension, new Filter.AllowAllFilter());
		updateResourceIdentifiables(hive, hiveScenario, partitionDimension, new Filter.AllowAllFilter());			
		updateMetaData(hiveScenario, hive, partitionDimension);
		commitReadonlyViolations(hiveScenario, hive, partitionDimension);
	}

	private static void updatePimaryIndexIdentifiables(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			Undoable undoable = new Undoable() {
				public void f() {
					//	Update the node of each primary key to another node, according to this map
					for (final PrimaryIndexIdentifiable primaryIndexInstance : iterateFilter.f(hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension))) {								
						final boolean readOnly = hive.getReadOnlyOfPrimaryIndexKey(partitionDimension.getName(), primaryIndexInstance.getPrimaryIndexKey());						
						try {
							updateReadOnly(primaryIndexInstance, !readOnly);
						} catch (Exception e) { throw new RuntimeException(e); }
						new Undo() { public void f() {							
							try {
								updateReadOnly(primaryIndexInstance, readOnly);
							} catch (Exception e) { throw new RuntimeException(e); }
						}};
					}
				}
				
				private void updateReadOnly(PrimaryIndexIdentifiable primaryIndexInstance, boolean toBool) throws HiveException, SQLException {
					hive.updatePrimaryIndexReadOnly(
							partitionDimension.getName(),
							primaryIndexInstance.getPrimaryIndexKey(),
							toBool);
					assertEquals(toBool, hive.getReadOnlyOfPrimaryIndexKey(partitionDimension.getName(), primaryIndexInstance.getPrimaryIndexKey()));
				}
			};
			undoable.cycle();
		} catch (Exception e)  { throw new HiveException("Undoable exception", e); }
	}
	
	private static void updateResourceIdentifiables(final Hive hive, final HiveScenario hiveScenario, final PartitionDimension partitionDimension, final Filter iterateFilter) throws HiveException {
		try {
			new Undoable() {
				public void f() {
					
					final Map<Object,Object> primaryIndexKeyToPrimaryIndexKeyMap = makeThisToThatMap(
							Transform.map(new Unary<PrimaryIndexIdentifiable,Object>() {
								public Object f(PrimaryIndexIdentifiable primaryIndexInstance) { return primaryIndexInstance.getPrimaryIndexKey(); }},
									hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension)));
					
					final Map<Object,Object> reversePrimaryIndexKeyToPrimaryIndexKeyMap = Transform.reverseMap(primaryIndexKeyToPrimaryIndexKeyMap);
								
					for (final Resource resource : iterateFilter.f(partitionDimension.getResources())) {
						
						Collection<ResourceIdentifiable> resourceIdentifiables = hiveScenario.getResourceIdentifiableInstancesForThisResource(resource);
						for (final ResourceIdentifiable resourceIdentifiable : resourceIdentifiables) {							
							final Object newPrimaryIndexKey = primaryIndexKeyToPrimaryIndexKeyMap.get(resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey());
							try {
								updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, resourceIdentifiable.getId(), newPrimaryIndexKey, resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey());																							
							} catch (Exception e) { throw new RuntimeException(e); }
							new Undo() { public void f() {						
								try {
									updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, resourceIdentifiable.getId(), reversePrimaryIndexKeyToPrimaryIndexKeyMap.get(newPrimaryIndexKey), newPrimaryIndexKey);											
								} catch (Exception e) { throw new RuntimeException(e); }
							}};	
						}
					}
				}
				private void updatePrimaryIndexKeyOfResourceId(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final Number resourceId, final Object newPrimaryIndexKey, final Object originalPrimaryIndexKey) throws HiveException, SQLException {
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
			final PrimaryIndexIdentifiable primaryIndexInstance = Atom.getFirst(hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension));
			final Resource resource = Atom.getFirst(partitionDimension.getResources());
			final SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());
			final ResourceIdentifiable resourceIdentifiable = Atom.getFirst(hiveScenario.getResourceIdentifiableInstancesForThisResource(resource));
			final SecondaryIndexIdentifiable secondaryIndexIdentifiable = Atom.getFirst(resourceIdentifiable.getSecondaryIndexIdentifiables());
			
			// Attempt to insert a secondary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.insertSecondaryIndexKey(secondaryIndex.getName(), resource.getName(), secondaryIndex.getResource().getPartitionDimension().getName(),
					Atom.getFirst(secondaryIndexIdentifiable.getResourceIdentifiable().generate(primaryIndexInstance).getSecondaryIndexIdentifiables()).getSecondaryIndexKey(),
					primaryIndexInstance.getPrimaryIndexKey());
			}}, HiveReadOnlyException.class);	
			
			// Attempt to insert a primary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.insertPrimaryIndexKey(partitionDimension.getName(), primaryIndexInstance.generate().getPrimaryIndexKey());
			}}, HiveReadOnlyException.class);	
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}	
	
	public void validateDeletesToPersistence(final HiveScenario hiveScenario) throws HiveException, SQLException
	{
		final Hive hive = hiveScenario.getHive();
		final PartitionDimension partitionDimension = hiveScenario.getCreatedPartitionDimension();					
		validateDeletePrimaryIndexKey(hiveScenario, hive, partitionDimension);	
		validateDeleteResourceIdentifiable(hiveScenario, hive, partitionDimension);
		validateDeleteSecondaryIndexIdentifiable(hiveScenario, hive, partitionDimension);
	}
	
	
	private void validateDeletePrimaryIndexKey(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		for (final PrimaryIndexIdentifiable primaryIndexIdentifiable : hiveScenario.getPrimaryIndexInstancesCreatedByThisPartitionDimension(partitionDimension)) {
			for (final Resource resource : partitionDimension.getResources()) {						
				new Undoable() { public void f() {
					try {
						hive.deletePrimaryIndexKey(partitionDimension.getName(), primaryIndexIdentifiable.getPrimaryIndexKey());
					} catch (Exception e) { throw new RuntimeException(e); }
					assertFalse(hive.doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexIdentifiable.getPrimaryIndexKey()));
				
					for (final ResourceIdentifiable resourceIdentifiable :
							Filter.grep(new Predicate<ResourceIdentifiable>() {
								public boolean f(ResourceIdentifiable resourceIdentifiable) {
									return resourceIdentifiable.getPrimaryIndexIdentifiable().equals(primaryIndexIdentifiable);
								}},
								hiveScenario.getResourceIdentifiableInstancesForThisResource(resource))) {	
						
						assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId()));
					
						for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : resourceIdentifiable.getSecondaryIndexIdentifiables()) {	
							final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexName());																								
							
							new Undo() { public void f()  {
								undoSecondaryIndexDelete(hive, partitionDimension, resource, resourceIdentifiable, secondaryIndex, secondaryIndexIdentifiable);
							}};								
						}
						
						new Undo() { public void f() {
							try {
								hive.insertResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId(), resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey());
							} catch (Exception e) { throw new RuntimeException(e); }
							assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId()));
						}};
					}
					
					new Undo() { public void f()  {				
						try {
							hive.insertPrimaryIndexKey(partitionDimension.getName(), primaryIndexIdentifiable.getPrimaryIndexKey());
						} catch (Exception e) { throw new RuntimeException(e); }
						assertTrue(hive.doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexIdentifiable.getPrimaryIndexKey()));
					}};
				}}.cycle();					
			}
		}
	}
	private void validateDeleteResourceIdentifiable(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		for (final Resource resource : partitionDimension.getResources()) {
			Collection<ResourceIdentifiable> resourceIdentifiables = hiveScenario.getResourceIdentifiableInstancesForThisResource(resource);
			for (final ResourceIdentifiable resourceIdentifiable : resourceIdentifiables) {
			
				// Test delete of a resource id and the cascade delete of its secondary index key
				try {
					new Undoable() {
						public void f() {
							try {
								hive.deleteResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId());
							} catch (Exception e) { throw new RuntimeException(e); }
							assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId()));
							
							for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : resourceIdentifiable.getSecondaryIndexIdentifiables()) {	
								final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexName());
								assertFalse(Filter.grepItemAgainstList(
										secondaryIndexIdentifiable.getResourceIdentifiable().getId(),
										hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexIdentifiable.getSecondaryIndexKey())));
							
								new Undo() { public void f()  {
									try {
										undoSecondaryIndexDelete(hive, partitionDimension, resource, resourceIdentifiable, secondaryIndex, secondaryIndexIdentifiable);
									} catch (Exception e) { throw new RuntimeException(e); }
								}};
							}
							
							new Undo() { public void f() {
								try {
									hive.insertResourceId(partitionDimension.getName(), resource.getName(), resourceIdentifiable.getId(), resourceIdentifiable.getPrimaryIndexIdentifiable().getPrimaryIndexKey());
								} catch (Exception e) { throw new RuntimeException(e); }
								assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), resourceIdentifiable.getId()));
							}};
					}}.cycle();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	private void validateDeleteSecondaryIndexIdentifiable(final HiveScenario hiveScenario, final Hive hive, final PartitionDimension partitionDimension) {
		for (final Resource resource : partitionDimension.getResources()) {
			Collection<ResourceIdentifiable> resourceIdentifiables = hiveScenario.getResourceIdentifiableInstancesForThisResource(resource);
			for (final ResourceIdentifiable resourceIdentifiable : resourceIdentifiables) {
				// Test delete of secondary index keys individually
				for (final SecondaryIndexIdentifiable secondaryIndexIdentifiable : resourceIdentifiable.getSecondaryIndexIdentifiables()) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexIdentifiable.getSecondaryIndexName());
					
					new Undoable() { 
						public void f() {
						
							try {
								hive.deleteSecondaryIndexKey(
									secondaryIndex.getName(),
									resource.getName(),
									partitionDimension.getName(),
									secondaryIndexIdentifiable.getSecondaryIndexKey(),
									secondaryIndexIdentifiable.getResourceIdentifiable().getId());
								assertFalse(Filter.grepItemAgainstList(secondaryIndexIdentifiable.getResourceIdentifiable().getId(),
												hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexIdentifiable.getSecondaryIndexKey())));
							} catch (Exception e) { throw new RuntimeException(e); }
							new Undo() { @SuppressWarnings("unchecked") public void f() {
								try {
									hive.insertSecondaryIndexKey(
											secondaryIndex.getName(),
											resource.getName(),
											partitionDimension.getName(),
											secondaryIndexIdentifiable.getSecondaryIndexKey(),
											secondaryIndexIdentifiable.getResourceIdentifiable().getId()
									);
								} catch (Exception e) { throw new RuntimeException(e); }
								assertTrue(Filter.grepItemAgainstList(secondaryIndexIdentifiable.getResourceIdentifiable().getPrimaryIndexIdentifiable().getPrimaryIndexKey(), 
									directory.getPrimaryIndexKeysOfSecondaryIndexKey(secondaryIndex, secondaryIndexIdentifiable.getSecondaryIndexKey())));
						}};
					}}.cycle(); 
				}
						
			}			
		}
	}
	
	private void undoSecondaryIndexDelete(final Hive hive, final PartitionDimension partitionDimension, final Resource resource, final ResourceIdentifiable resourceIdentifiable, final SecondaryIndex secondaryIndex, final SecondaryIndexIdentifiable secondaryIndexIdentifiable) {
		try {
			hive.insertSecondaryIndexKey(secondaryIndex.getName(), resource.getName(), partitionDimension.getName(),
					secondaryIndexIdentifiable.getSecondaryIndexKey(), resourceIdentifiable.getId());
		} catch (Exception e) { throw new RuntimeException(e); }	
		assertTrue(Filter.grepItemAgainstList(
			secondaryIndexIdentifiable.getResourceIdentifiable().getId(),
			hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexIdentifiable.getSecondaryIndexKey())));
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
