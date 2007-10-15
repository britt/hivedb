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
import org.hivedb.meta.EntityConfig;
import org.hivedb.meta.EntityGeneratorImpl;
import org.hivedb.meta.EntityIndexConfig;
import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;
import org.hivedb.meta.PrimaryIndexKeyGenerator;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.JdbcTypeMapper;
import org.hivedb.util.Persister;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Undoable;
import org.hivedb.util.scenarioBuilder.HiveScenario;

public class HiveScenarioTest {
	
	HiveConfig hiveConfig;
	Collection<Node> dataNodes;
	private NodeResolver directory;
	public HiveScenarioTest(HiveConfig hiveConfig)
	{
		this.hiveConfig = hiveConfig;
	}
	public void performTest(int primaryIndexInstanceCount, int resourceInstanceCount, Persister persister) {
		HiveScenario hiveScenario = HiveScenario.run(hiveConfig, primaryIndexInstanceCount, resourceInstanceCount, persister);
		validate(hiveScenario);
	}

	protected void validate(HiveScenario hiveScenario) {
		try {
			validateHiveMetadata(hiveScenario.getHiveConfig());
			// Validate CRUD operations. Read at the beginning and after updates
			// to verify that the update restored the data to its original state.
			validateReadsFromPersistence(hiveScenario.getHiveConfig(), hiveScenario.getGeneratedResourceInstances());
			validateUpdatesToPersistence(hiveScenario.getHiveConfig(), hiveScenario.getGeneratedResourceInstances());
			validateReadsFromPersistence(hiveScenario.getHiveConfig(), hiveScenario.getGeneratedResourceInstances());
			validateDeletesToPersistence(hiveScenario.getHiveConfig(), hiveScenario.getGeneratedResourceInstances());
			// data is reinserted after deletes but nodes can change so we can't validate equality again
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void validateHiveMetadata(final HiveConfig hiveConfig) throws HiveException, SQLException
	{
		Hive hive = hiveConfig.getHive();
		String partitionDimensionName = hiveConfig.getEntityConfig().getPartitionDimensionName();
		PartitionDimension expectedPartitionDimension = PartitionDimensionCreator.create(hiveConfig);
		PartitionDimension actualPartitionDimension = hive.getPartitionDimension(partitionDimensionName);
		
		// Validate our PartitionDimension in memory against those that are in the persistence
		// This tests all the hive metadata.
		assertEquals(String.format("Expected %s but got %s", hive.getPartitionDimension(partitionDimensionName), hive.getPartitionDimensions()),			
			actualPartitionDimension,
			expectedPartitionDimension);
	}
	
	@SuppressWarnings("unchecked")
	public void validateReadsFromPersistence(final HiveConfig hiveConfig, Collection<Object> resourceInstances) throws HiveException, SQLException
	{
		Hive hive = hiveConfig.getHive();
		String partitionDimensionName = hiveConfig.getEntityConfig().getPartitionDimensionName();
		PartitionDimension expectedPartitionDimension = PartitionDimensionCreator.create(hiveConfig);
		final PartitionDimension actualPartitionDimension = hive.getPartitionDimension(partitionDimensionName);
		
		Collection<Object> primaryIndexKeys = getGeneratedPrimaryIndexKeys(hiveConfig, resourceInstances);
		directory = new Directory(actualPartitionDimension,new HiveBasicDataSource(hive.getUri()));
		for (Object primaryindexKey : primaryIndexKeys)
			assertTrue(directory.getNodeSemamphoresOfPrimaryIndexKey(primaryindexKey).size() > 0);
		
		assertEquals(
					new TreeSet<Resource>(expectedPartitionDimension.getResources()),
					new TreeSet<Resource>(actualPartitionDimension.getResources()));
		
		// Validate that the secondary index keys are in the database with the right primary index key
		for (final Resource resource : actualPartitionDimension.getResources()) {
			for (final Object resourceInstance : resourceInstances) {
				final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();					
				Collection<? extends EntityIndexConfig> secondaryIndexConfigs = entityConfig.getEntitySecondaryIndexConfigs();
				for (EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
					
					//  Assert that querying for all the secondary index keys of a primary index key returns the right collection
					final List<Object> secondaryIndexKeys = new ArrayList<Object>(hive.getSecondaryIndexKeysWithResourceId(
																secondaryIndex.getName(),
																resource.getName(),
																actualPartitionDimension.getName(),
																entityConfig.getId(resourceInstance)));
					
					Collection<Object> expectedSecondaryIndexKeys = secondaryIndexConfig.getIndexValues(resourceInstance);
					for (Object expectedSecondaryIndexKey : expectedSecondaryIndexKeys) {
						
							assertTrue(String.format("directory.getSecondaryIndexKeysWithPrimaryKey(%s,%s,%s,%s)", secondaryIndex.getName(), resource.getName(), actualPartitionDimension.getName(), expectedSecondaryIndexKey),
									secondaryIndexKeys.contains(expectedSecondaryIndexKey));
					
							Collection<KeySemaphore> nodeSemaphoreOfSecondaryIndexKeys = directory.getNodeSemaphoresOfSecondaryIndexKey(secondaryIndex, expectedSecondaryIndexKey);
							assertTrue(String.format("directory.getNodeSemaphoresOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), expectedSecondaryIndexKey),
									   nodeSemaphoreOfSecondaryIndexKeys.size() > 0);
								
							// Assert that querying for the primary key of the secondary index key yields what we expect
							Object expectedPrimaryIndexKey = entityConfig.getPrimaryIndexKey(resourceInstance);
							Collection<Object> actualPrimaryIndexKeys = directory.getPrimaryIndexKeysOfSecondaryIndexKey(
									secondaryIndex,
									expectedSecondaryIndexKey);
							assertTrue(String.format("directory.getPrimaryIndexKeysOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), expectedSecondaryIndexKey),
									Filter.grepItemAgainstList(expectedPrimaryIndexKey, actualPrimaryIndexKeys));
						
							// Assert that one of the nodes of the secondary index key is the same as that of the primary index key
							// There are multiple nodes returned when multiple primray index keys exist for a secondary index key
							Collection<KeySemaphore> nodeSemaphoreOfPrimaryIndexKey = directory.getNodeSemamphoresOfPrimaryIndexKey(expectedPrimaryIndexKey);
							for(KeySemaphore semaphore : nodeSemaphoreOfPrimaryIndexKey)
								assertTrue(Filter.grepItemAgainstList(semaphore, nodeSemaphoreOfSecondaryIndexKeys));	
					}	
				}
			}	
		}
	}
	private static Collection<Object> getGeneratedPrimaryIndexKeys(final HiveConfig hiveConfig, Collection<Object> resourceInstances) {
		return Transform.map(new Unary<Object,Object>() {
			public Object f(Object resourceInstance) {
				return hiveConfig.getEntityConfig().getPrimaryIndexKey(resourceInstance);
			}
		}, resourceInstances);
	}

	public void validateUpdatesToPersistence(final HiveConfig hiveConfig, Collection<Object> resourceInstances) throws HiveException, SQLException
	{
		updatePimaryIndexKeys(hiveConfig, resourceInstances, new Filter.AllowAllFilter());
		updatePrimaryIndexKeyOfResource(hiveConfig, resourceInstances, new Filter.AllowAllFilter());			
		updateMetaData(hiveConfig, resourceInstances);
		commitReadonlyViolations(hiveConfig, resourceInstances);
	}

	private static void updatePimaryIndexKeys(final HiveConfig hiveConfig, final Collection<Object> resourceInstances, final Filter iterateFilter) throws HiveException {
		final Hive hive = hiveConfig.getHive();
		final String partitionDimensionName = hiveConfig.getEntityConfig().getPartitionDimensionName();
		
		try {
			Undoable undoable = new Undoable() {
				public void f() {
					//	Update the node of each primary key to another node, according to this map
					for (final Object primaryIndexKey : iterateFilter.f(getGeneratedPrimaryIndexKeys(hiveConfig, resourceInstances))) {								
						final boolean readOnly = hive.getReadOnlyOfPrimaryIndexKey(partitionDimensionName, primaryIndexKey);						
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
							partitionDimensionName,
							primaryIndexKey,
							toBool);
					assertEquals(toBool, hive.getReadOnlyOfPrimaryIndexKey(partitionDimensionName, primaryIndexKey));
				}
			};
			undoable.cycle();
		} catch (Exception e)  { throw new HiveException("Undoable exception", e); }
	}
	
	private static void updatePrimaryIndexKeyOfResource(final HiveConfig hiveConfig, final Collection<Object> resourceInstances, final Filter iterateFilter) throws HiveException {
		
		final Hive hive = hiveConfig.getHive();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(hiveConfig.getEntityConfig().getPartitionDimensionName());
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		if (resource.isPartitioningResource())
			return;
		new Undoable() {
			public void f() {
				
				final Map<Object,Object> primaryIndexKeyToPrimaryIndexKeyMap = makeThisToThatMap(getGeneratedPrimaryIndexKeys(hiveConfig, resourceInstances));																
				final Map<Object,Object> reversePrimaryIndexKeyToPrimaryIndexKeyMap = Transform.reverseMap(primaryIndexKeyToPrimaryIndexKeyMap);
							
				for (final Object resourceInstance : resourceInstances) {							
					
					final Object primaryIndexKey = entityConfig.getPrimaryIndexKey(resourceInstance);
					final Object newPrimaryIndexKey = primaryIndexKeyToPrimaryIndexKeyMap.get(primaryIndexKey);
					try {
						updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, entityConfig.getId(resourceInstance), newPrimaryIndexKey, primaryIndexKey);																							
					} catch (Exception e) { throw new RuntimeException(e); }
					new Undo() { public void f() {						
						try {
							updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, entityConfig.getId(resourceInstance), reversePrimaryIndexKeyToPrimaryIndexKeyMap.get(newPrimaryIndexKey), newPrimaryIndexKey);											
						} catch (Exception e) { throw new RuntimeException(e); }
					}};	
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
		
	}
	
	private static void updateMetaData(final HiveConfig hiveConfig, Collection<Object> resourceInstances)
	{
		final Hive hive = hiveConfig.getHive();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(hiveConfig.getEntityConfig().getPartitionDimensionName());
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		try {
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
	
	private static void commitReadonlyViolations(final HiveConfig hiveConfig, Collection<Object> resourceInstances) throws HiveException 
	{
		final Hive hive = hiveConfig.getHive();
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(entityConfig.getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		
		try {
			final Object primaryIndexKey = Atom.getFirst(getGeneratedPrimaryIndexKeys(hiveConfig, resourceInstances));
			final SecondaryIndex secondaryIndex = Atom.getFirst(resource.getSecondaryIndexes());			
			
			// Attempt to insert a secondary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				Object newResourceInstance = new EntityGeneratorImpl<Object>(entityConfig)
										.generate(primaryIndexKey);
				Collection<? extends EntityIndexConfig> secondaryIndexConfigs = 
					entityConfig .getEntitySecondaryIndexConfigs();
				final Object secondaryIndexValue = Atom.getFirst(secondaryIndexConfigs).getIndexValues(newResourceInstance);
				hive.insertSecondaryIndexKey(
						secondaryIndex.getName(), 
						resource.getName(), 
						secondaryIndex.getResource().getPartitionDimension().getName(),
						secondaryIndexValue,
						primaryIndexKey);
			}}, HiveReadOnlyException.class);	
			
			// Attempt to insert a primary index key
			AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
				hive.updateHiveReadOnly(true);
				new Undo() { public void f() throws Exception {
					hive.updateHiveReadOnly(false);
				}};
				hive.insertPrimaryIndexKey(partitionDimension.getName(), new PrimaryIndexKeyGenerator(entityConfig).generate());
			}}, HiveReadOnlyException.class);	
		} catch (Exception e) { throw new HiveException("Undoable exception", e); }
	}	
	
	public void validateDeletesToPersistence(final HiveConfig hiveConfig, Collection<Object> resourceInstances) throws HiveException, SQLException
	{	
		validateDeletePrimaryIndexKey(hiveConfig, resourceInstances);	
		validateDeleteResourceInstances(hiveConfig, resourceInstances);
		validateDeleteSecondaryIndexKeys(hiveConfig, resourceInstances);
	}
	
	
	private void validateDeletePrimaryIndexKey(final HiveConfig hiveConfig, final Collection<Object> resourceInstances) {
		final Hive hive = hiveConfig.getHive();
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(entityConfig.getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		
		for (final Object primaryIndexKey : getGeneratedPrimaryIndexKeys(hiveConfig, resourceInstances)) {			
			new Undoable() { public void f() {
				try {
					hive.deletePrimaryIndexKey(partitionDimension.getName(), primaryIndexKey);
				} catch (Exception e) { throw new RuntimeException(e); }
				assertFalse(hive.doesPrimaryIndexKeyExist(partitionDimension.getName(), primaryIndexKey));
			
				for (final Object resourceInstance :
						Filter.grep(new Predicate<Object>() {
							public boolean f(Object resourceInstance) {
								return entityConfig.getPrimaryIndexKey(resourceInstance).equals(primaryIndexKey);
							}},
							resourceInstances)) {	
					
					assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), entityConfig.getId(resourceInstance)));
				
					Collection<? extends EntityIndexConfig> secondaryIndexConfigs = entityConfig.getEntitySecondaryIndexConfigs();
					for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
					
						new Undo() { public void f()  {
							undoSecondaryIndexDelete(
									hiveConfig,
									secondaryIndexConfig,
									resourceInstance);
						}};								
					}
					
					new Undo() { public void f() {
						try {
							hive.insertResourceId(partitionDimension.getName(), resource.getName(), entityConfig.getId(resourceInstance), entityConfig.getPrimaryIndexKey(resourceInstance));
						} catch (Exception e) { throw new RuntimeException(e); }
						assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), entityConfig.getId(resourceInstance)));
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
	private void validateDeleteResourceInstances(final HiveConfig hiveConfig, final Collection<Object> resourceInstances) {
		final Hive hive = hiveConfig.getHive();
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(entityConfig.getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
	
		if (resource.isPartitioningResource()) 
			return;
		for (final Object resourceInstance : resourceInstances) {
			// Test delete of a resource id and the cascade delete of its secondary index key
			try {
				new Undoable() {
					public void f() {
						try {
							hive.deleteResourceId(partitionDimension.getName(), resource.getName(), entityConfig.getId(resourceInstance));
						} catch (Exception e) { throw new RuntimeException(e); }
						assertFalse(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), entityConfig.getId(resourceInstance)));
						
						Collection<? extends EntityIndexConfig> secondaryIndexConfigs = entityConfig.getEntitySecondaryIndexConfigs();
						for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
							final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
							Object secondaryIndexValue = secondaryIndexConfig.getIndexValues(resourceInstance);
							new Actor(secondaryIndexValue) {
								public void f(Object secondaryIndexKey){
									assertFalse(Filter.grepItemAgainstList(
											entityConfig.getId(resourceInstance),
											hive.getResourceIdsOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey)));
								}
							};
							
						
							new Undo() { public void f()  {
								try {
									undoSecondaryIndexDelete(hiveConfig, secondaryIndexConfig, resourceInstance);
								} catch (Exception e) { throw new RuntimeException(e); }
							}};
						}
						
						new Undo() { public void f() {
							try {
								hive.insertResourceId(partitionDimension.getName(), resource.getName(), entityConfig.getId(resourceInstance), entityConfig.getPrimaryIndexKey(resourceInstance));
							} catch (Exception e) { throw new RuntimeException(e); }
							assertTrue(hive.doesResourceIdExist(resource.getName(), partitionDimension.getName(), entityConfig.getId(resourceInstance)));
						}};
				}}.cycle();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	
	}
	private void validateDeleteSecondaryIndexKeys(final HiveConfig hiveConfig, final Collection<Object> resourceInstances) {
		final Hive hive = hiveConfig.getHive();
		final EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(entityConfig.getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		
		for (final Object resourceInstance : resourceInstances) {
			// Test delete of secondary index keys individually
			Collection<? extends EntityIndexConfig> secondaryIndexConfigs = entityConfig.getEntitySecondaryIndexConfigs();
			for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
				final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
				
				new Undoable() { 
					public void f() {
						final Object resourceId = entityConfig.getId(resourceInstance);
						final Object secondaryIndexValue = secondaryIndexConfig.getIndexValues(resourceInstance);
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
	
	private void undoSecondaryIndexDelete(
			final HiveConfig hiveConfig, 
			final EntityIndexConfig secondaryIndexConfig,
			final Object resourceInstance) {
		final Hive hive = hiveConfig.getHive();
		EntityConfig<Object> entityConfig = hiveConfig.getEntityConfig();
		final PartitionDimension partitionDimension = hive.getPartitionDimension(entityConfig.getPartitionDimensionName());
		final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());																								
		final Object resourceId = entityConfig.getId(resourceInstance);
		final Object secondaryIndexValue = secondaryIndexConfig.getIndexValues(resourceInstance);
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
