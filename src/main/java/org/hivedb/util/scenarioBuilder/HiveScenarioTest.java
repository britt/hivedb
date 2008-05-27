package org.hivedb.util.scenarioBuilder;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveLockableException;
import org.hivedb.Lockable.Status;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.EntityGeneratorImpl;
import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.ColumnInfo;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.AssertUtils;
import org.hivedb.util.QuickCache;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.classgen.GeneratePrimitiveValue;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Actor;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Predicate;
import org.hivedb.util.functional.RingIteratorable;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Undoable;
import org.testng.Assert;

public class HiveScenarioTest {
	protected final Hive hive;
	protected final EntityHiveConfig entityHiveConfig;
	protected final Class representedInterface;
	protected final DataAccessObject<Object, Serializable> dataAccessObject;
	static QuickCache primitiveGenerators = new QuickCache(); // cache generators for sequential randomness

	public HiveScenarioTest(EntityHiveConfig entityHiveConfig, Hive hive, Class representedInterface, DataAccessObject<Object, Serializable> dataAccessObject)
	{
		this.hive = hive;
		this.entityHiveConfig = entityHiveConfig;
		this.representedInterface = representedInterface;
		this.dataAccessObject = dataAccessObject;
	}
	public void performTest(int primaryIndexInstanceCount, int resourceInstanceCount) {
		HiveScenario hiveScenario = HiveScenario.run(entityHiveConfig, representedInterface, primaryIndexInstanceCount, resourceInstanceCount, dataAccessObject);
		new Validate(hiveScenario.getGeneratedResourceInstances()).run();
	}

	private class Validate {
		private Collection<Object> resourceInstances;
		public Validate(Collection<Object> resourceInstances) {
			this.resourceInstances = resourceInstances;
		}
		public void run() {
			try {
				validateHiveMetadata();		
				// Validate CRUD operations. Read at the beginning and after updates
				// to verify that the update restored the data to its original state
				validateReadsFromPersistence();
				validateUpdatesToPersistence();
				validateReadsFromPersistence();
				validateDeletesToPersistence();
				// data is reinserted after deletes but nodes can change so we can't validate equality again
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		
		private void validateHiveMetadata() throws HiveException, SQLException
		{
			final String resourceName = entityHiveConfig.getEntityConfig(representedInterface).getResourceName();
			Resource expectedResource = PartitionDimensionCreator.create(entityHiveConfig, hive.getUri()).getResource(resourceName);
			Resource actualResource = hive.getPartitionDimension().getResource(entityHiveConfig.getEntityConfig(representedInterface).getResourceName());
			
			// Validate our PartitionDimension in memory against those that are in the persistence
			// This tests all the hive metadata.
			assertEquals(String.format("Expected %s but got %s", actualResource, hive.getPartitionDimension().getResource(resourceName)),			
				actualResource,
				expectedResource);
		}
		
		@SuppressWarnings("unchecked")
		private void validateReadsFromPersistence() throws HiveException, SQLException
		{
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = hive.getPartitionDimension().getResource(entityHiveConfig.getEntityConfig(representedInterface).getResourceName());
			Collection<Object> primaryIndexKeys = getGeneratedPrimaryIndexKeys();
			Directory directory = new Directory(partitionDimension, CachingDataSourceProvider.getInstance().getDataSource(hive.getUri()));
			for (Object primaryindexKey : primaryIndexKeys)
				assertTrue(directory.getKeySemamphoresOfPrimaryIndexKey(primaryindexKey).size() > 0);
			
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
				
			for (final Object resourceInstance : resourceInstances) {
				final Object loadedResourceInstance = dataAccessObject.get(entityConfig.getId(resourceInstance));
				Assert.assertEquals(loadedResourceInstance.hashCode(), resourceInstance.hashCode(),
						ReflectionTools.getDifferingFields(resourceInstance, loadedResourceInstance, (Class<Object>)entityConfig.getRepresentedInterface()).toString());
				Object actualPrimaryIndexKey = null;
				
				// Make sure that non partitioning resources instances have the correct primary index key
				if (!entityConfig.isPartitioningResource()) {
					actualPrimaryIndexKey = hive.directory().getPrimaryIndexKeyOfResourceId(resource.getName(), entityConfig.getId(resourceInstance));					
					Object expectedPrimaryIndexKey = entityConfig.getPrimaryIndexKey(resourceInstance);
				
					assertEquals(String.format("directory.getPrimaryIndexKeyOfResourceId(%s,%s)", resource.getName(), entityConfig.getId(resourceInstance)),
						actualPrimaryIndexKey, expectedPrimaryIndexKey);
				}
				
				Collection<? extends EntityIndexConfig> secondaryIndexConfigs = getHiveIndexes();
				for (EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
					
					//  Assert that querying for all the secondary index keys of a primary index key returns the right collection
					final List<Object> secondaryIndexKeys = 
						new ArrayList<Object>(
								hive.directory().getSecondaryIndexKeysWithResourceId(
										resource.getName(),
										secondaryIndex.getName(),
										entityConfig.getId(resourceInstance)));
					
					Collection<Object> expectedSecondaryIndexKeys = secondaryIndexConfig.getIndexValues(resourceInstance);
					for (Object expectedSecondaryIndexKey : expectedSecondaryIndexKeys) {
						
							assertTrue(String.format("directory.getSecondaryIndexKeysWithPrimaryKey(%s,%s,%s,%s)", secondaryIndex.getName(), resource.getName(), partitionDimension.getName(), expectedSecondaryIndexKey),
									secondaryIndexKeys.contains(expectedSecondaryIndexKey));
					
							Collection<KeySemaphore> keySemaphoreOfSecondaryIndexKeys = directory.getKeySemaphoresOfSecondaryIndexKey(secondaryIndex, expectedSecondaryIndexKey);
							assertTrue(String.format("directory.getKeySemaphoresOfSecondaryIndexKey(%s,%s)", secondaryIndex.getName(), expectedSecondaryIndexKey),
									   keySemaphoreOfSecondaryIndexKeys.size() > 0);
								
							// Assert that querying for the primary key of the secondary index key yields what we expect
							Object expectedPrimaryIndexKey = entityConfig.getPrimaryIndexKey(resourceInstance);
							Collection<Object> actualPrimaryIndexKeys = directory.getPrimaryIndexKeysOfSecondaryIndexKey(
									secondaryIndex,
									expectedSecondaryIndexKey);
								
							assertTrue(String.format("directory.getPrimaryIndexKeysOfSecondaryIndexKey(%s,%s): expected %s got %s", secondaryIndex.getName(), expectedSecondaryIndexKey, expectedPrimaryIndexKey, actualPrimaryIndexKeys),
									Filter.grepItemAgainstList(expectedPrimaryIndexKey, actualPrimaryIndexKeys));
							
							// Assert that one of the nodes of the secondary index key is the same as that of the primary index key
							// There are multiple nodes returned when multiple primary index keys exist for a secondary index key
							Collection<KeySemaphore> keySemaphoreOfPrimaryIndexKey = directory.getKeySemamphoresOfPrimaryIndexKey(expectedPrimaryIndexKey);
							for(KeySemaphore semaphore : keySemaphoreOfPrimaryIndexKey)
								assertTrue(Filter.grepItemAgainstList(semaphore, keySemaphoreOfSecondaryIndexKeys));	
					}	
				}
			}	
		}
		private Collection<Object> getGeneratedPrimaryIndexKeys() {
			return Transform.map(new Unary<Object,Object>() {
				public Object f(Object resourceInstance) {
					return entityHiveConfig.getEntityConfig(representedInterface).getPrimaryIndexKey(resourceInstance);
				}
			}, resourceInstances);
		}
		
		private void validateUpdatesToPersistence() throws HiveException, SQLException
		{
			updatePimaryIndexKeys(new Filter.AllowAllFilter());
			updatePrimaryIndexKeyOfResource(new Filter.AllowAllFilter());			
			// TODO something mysterious fails here during the H2 test. I can't figure it out after extensive
			//updateMetaData(hiveConfig, resourceInstances);
			updatePropertiesDelegatedToOtherResources();
			commitReadonlyViolations(entityHiveConfig,hive,representedInterface, resourceInstances);
		}
		
		private void updatePimaryIndexKeys(final Filter iterateFilter) throws HiveException {
			// The only thing to do to primary index keys is to make them read-only
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
			try {
				Undoable undoable = new Undoable() {
					public void f() {
						for (final Object primaryIndexKey : iterateFilter.f(getGeneratedPrimaryIndexKeys())) {								
							final boolean readOnly = hive.directory().getReadOnlyOfPrimaryIndexKey(primaryIndexKey);
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
						hive.directory().updatePrimaryIndexKeyReadOnly(primaryIndexKey, toBool);
						assertEquals(toBool, hive.directory().getReadOnlyOfPrimaryIndexKey(primaryIndexKey));
					}
				};
				undoable.cycle();
			} catch (Exception e)  { throw new HiveException("Undoable exception", e); }
		}
		
		private void updatePrimaryIndexKeyOfResource(final Filter iterateFilter) throws HiveException {
			
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			if (resource.isPartitioningResource())
				return;
			new Undoable() {
				public void f() {
					
					final Map<Object,Object> primaryIndexKeyToPrimaryIndexKeyMap = makeThisToThatMap(getGeneratedPrimaryIndexKeys());																
					final Map<Object,Object> reversePrimaryIndexKeyToPrimaryIndexKeyMap = Transform.reverseMap(primaryIndexKeyToPrimaryIndexKeyMap);
								
					for (final Object resourceInstance : resourceInstances) {							
						
						final Object primaryIndexKey = entityConfig.getPrimaryIndexKey(resourceInstance);
						final Object newPrimaryIndexKey = primaryIndexKeyToPrimaryIndexKeyMap.get(primaryIndexKey);
						try {
							updatePrimaryIndexKeyOfResourceInstance(
								entityConfig, resourceInstance,  newPrimaryIndexKey);
							//updatePrimaryIndexKeyOfResourceId(hive, partitionDimension, resource, entityConfig.getId(resourceInstance), newPrimaryIndexKey, primaryIndexKey);																							
						} catch (Exception e) { throw new RuntimeException(e); }
						new Undo() { public void f() {						
							try {
								updatePrimaryIndexKeyOfResourceInstance(
									entityConfig, resourceInstance, primaryIndexKey);
							} catch (Exception e) { throw new RuntimeException(e); }
						}};	
					}	
				}

				private void updatePrimaryIndexKeyOfResourceInstance(
						final EntityConfig entityConfig,
						final Object resourceInstance,
						final Object newPrimaryIndexKey) {
					// TODO temporary work around to moving between nodes	
					final Serializable id = entityConfig.getId(resourceInstance);
					Object mutableResourceInstance = dataAccessObject.get(id);
					dataAccessObject.delete(id);
					GeneratedInstanceInterceptor.setProperty(mutableResourceInstance, entityConfig.getPrimaryIndexKeyPropertyName(), newPrimaryIndexKey);
					dataAccessObject.save(mutableResourceInstance);
					Assert.assertNotNull(dataAccessObject.get(id));
					assertEquals(
							newPrimaryIndexKey, 
							hive.directory().getPrimaryIndexKeyOfResourceId(resource.getName(), id));
				}
			}.cycle();	
		}
		
		private void updatePropertiesDelegatedToOtherResources() {
			
		}
		
		private void updateMetaData()
		{
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			try {
				new Undoable() {
					public void f() {						
						final String name = partitionDimension.getName();			
						final Assigner assigner = hive.getAssigner();
						final int columnType = partitionDimension.getColumnType();
						final String indexUri = partitionDimension.getIndexUri();
						hive.setAssigner(new Assigner() {
							public Node chooseNode(Collection<Node> nodes, Object value) {
								return null;
							}
		
							public Collection<Node> chooseNodes(Collection<Node> nodes, Object value) {
								return Arrays.asList(new Node[]{chooseNode(nodes,value)});
							}				
						});
						partitionDimension.setColumnType(JdbcTypeMapper.parseJdbcType(JdbcTypeMapper.FLOAT));
						try {
							hive.updatePartitionDimension(partitionDimension);
						} catch (Exception e) { throw new RuntimeException(e); }
							
						assertEquality(hive, partitionDimension);
						
						new Undo() {							
							public void f() {
								partitionDimension.setName(name);
								partitionDimension.setColumnType(columnType);
								hive.setAssigner(assigner);
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
						final Node node = Atom.getFirstOrThrow(hive.getNodes());
						final Status status = node.getStatus();
						final String host = node.getHost();
						final String db = node.getDatabaseName();
						final String user = node.getUsername();
						final String pw = node.getPassword();
						node.setHost("arb");
						node.setDatabaseName("it");
						node.setUsername("ra");
						node.setPassword("ry");
						try {
							hive.updateNode(node);			
						} catch (Exception e) { throw new RuntimeException(e); }
						assertEquality(hive, node);
						new Undo() {							
							public void f() {
								node.setHost(host);
								node.setDatabaseName(db);
								node.setUsername(user);
								node.setPassword(pw);
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
						final Integer revision = hive.getRevision();
						final String name = resource.getName();	
		
						resource.setName("X");
						try {
							hive.updateResource(resource);
						} catch (Exception e) { throw new RuntimeException(e); }
						assertEquality(hive, resource);
						assertEquals(revision+1, hive.getRevision());
						
						new Undo() {							
							public void f() {
								resource.setName(name);			
								try {
									hive.updateResource(resource);
								} catch (Exception e) { throw new RuntimeException(e); }
								assertEquality(hive, resource);
								assertEquals(revision+2, hive.getRevision());
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
						
						assertEquality(hive, resource, secondaryIndex);
						
						new Undo() {							
							public void f() {
								secondaryIndex.setColumnInfo(columnInfo);
								try {
									hive.updateSecondaryIndex(secondaryIndex);
								} catch (Exception e) { throw new RuntimeException(e); }
								assertEquality(hive, resource, secondaryIndex);
							}
						};
					}
					
				}.cycle();
			}
			catch (Exception e) { throw new RuntimeException("Undoable exception", e); }
											
		}
		
		
		
		
		private void commitReadonlyViolations(final EntityHiveConfig enityHiveConfig, final Hive hive, Class representedInterface, Collection<Object> resourceInstances) throws HiveException 
		{
			final EntityConfig entityConfig = enityHiveConfig.getEntityConfig(representedInterface);
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			
			try {
				final Object primaryIndexKey = Atom.getFirst(getGeneratedPrimaryIndexKeys());
				final SecondaryIndex secondaryIndex = Atom.getFirstOrNull(resource.getSecondaryIndexes());			
				if (secondaryIndex != null) {
					// Attempt to insert a secondary index key
					AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
						hive.updateHiveStatus(Status.readOnly);
						new Undo() { public void f() throws Exception {
							hive.updateHiveStatus(Status.writable);
						}};
						Object newResourceInstance = new EntityGeneratorImpl<Object>(entityConfig)
												.generate(primaryIndexKey);
						Collection<? extends EntityIndexConfig> secondaryIndexConfigs = getHiveIndexes();
						final Collection<Object> secondaryIndexKeys = Atom.getFirst(secondaryIndexConfigs).getIndexValues(newResourceInstance);
						for (final Object secondaryIndexKey : secondaryIndexKeys)
							hive.directory().insertSecondaryIndexKey(
								resource.getName(),
								secondaryIndex.getName(), 
								secondaryIndexKey,
								entityConfig.getId(newResourceInstance));
					}}, HiveLockableException.class);	
				}
				// Attempt to insert a primary index key
				AssertUtils.assertThrows(new AssertUtils.UndoableToss() { public void f() throws Exception {				
					hive.updateHiveStatus(Status.readOnly);
					new Undo() { public void f() throws Exception {
						hive.updateHiveStatus(Status.writable);
					}};
					hive.directory().insertPrimaryIndexKey(
							primitiveGenerators.get(entityConfig.getPrimaryIndexKeyPropertyName(), new Delay<Generator>() {
								public Generator f() {
									return new GeneratePrimitiveValue(ReflectionTools.getPropertyType(
											entityConfig.getRepresentedInterface(),
											entityConfig.getPrimaryIndexKeyPropertyName()));
								}}).generate());
							
							
				}}, HiveLockableException.class);	
			} catch (Exception e) { throw new HiveException("Undoable exception", e); }
		}	
		private Collection<? extends EntityIndexConfig> getHiveIndexes() {
			return Filter.grep(new Predicate<EntityIndexConfig>() {
				public boolean f(EntityIndexConfig entityIndexConfig) {
					return entityIndexConfig.getIndexType().equals(IndexType.Hive);	
				}}, entityHiveConfig.getEntityConfig(representedInterface).getEntityIndexConfigs());
		}
		
		private void validateDeletesToPersistence() throws HiveException, SQLException
		{	
			//validateDeletePrimaryIndexKey(entityHiveConfig, hive,  representedInterface, resourceInstances);	
			validateDeleteResourceInstances(entityHiveConfig, hive, representedInterface, resourceInstances);
			validateDeleteSecondaryIndexKeys(entityHiveConfig, hive, representedInterface, resourceInstances);
		}
		
		private void validateDeletePrimaryIndexKey(final EntityHiveConfig entityHiveConfig, final Hive hive, Class representedInterface, final Collection<Object> resourceInstances) {
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			
			for (final Object primaryIndexKey : getGeneratedPrimaryIndexKeys()) {			
				new Undoable() { public void f() {
					try {
						hive.directory().deletePrimaryIndexKey(primaryIndexKey);
					} catch (Exception e) { throw new RuntimeException(e); }
					assertFalse(hive.directory().doesPrimaryIndexKeyExist(primaryIndexKey));
				
					for (final Object resourceInstance :
							Filter.grep(new Predicate<Object>() {
								public boolean f(Object resourceInstance) {
									return entityConfig.getPrimaryIndexKey(resourceInstance).equals(primaryIndexKey);
								}},
								resourceInstances)) {	
						
						assertFalse(hive.directory().doesResourceIdExist(resource.getName(), entityConfig.getId(resourceInstance)));
					
						Collection<? extends EntityIndexConfig> secondaryIndexConfigs = Filter.grep(new Predicate<EntityIndexConfig>() {
							public boolean f(EntityIndexConfig entityIndexConfig) {
								return !entityIndexConfig.getIndexType().equals(IndexType.Delegates);
							}}, getHiveIndexes());
						
						for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
							new Undo() { public void f()  {
								undoSecondaryIndexDelete(
										hive,
										entityConfig,
										secondaryIndexConfig,
										resourceInstance);
							}};								
						}
						
						new Undo() { public void f() {
							try {
								hive.directory().insertResourceId(resource.getName(), entityConfig.getId(resourceInstance), entityConfig.getPrimaryIndexKey(resourceInstance));
							} catch (Exception e) { throw new RuntimeException(e); }
							assertTrue(hive.directory().doesResourceIdExist(resource.getName(), entityConfig.getId(resourceInstance)));
						}};
					}
					
					new Undo() { public void f()  {				
						try {
							hive.directory().insertPrimaryIndexKey(primaryIndexKey);
						} catch (Exception e) { throw new RuntimeException(e); }
						assertTrue(hive.directory().doesPrimaryIndexKeyExist(primaryIndexKey));
					}};
				}}.cycle();					
			}
		}
		private void validateDeleteResourceInstances(final EntityHiveConfig entityHiveConifg, final Hive hive, Class representedInterface, final Collection<Object> resourceInstances) {
			final EntityConfig entityConfig = entityHiveConifg.getEntityConfig(representedInterface);
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
		
			if (resource.isPartitioningResource()) 
				return;
			for (final Object resourceInstance : resourceInstances) {
				// Test delete of a resource id and the cascade delete of its secondary index key
				try {
					new Undoable() {
						public void f() {
							try {
								if (dataAccessObject.get(entityConfig.getId(resourceInstance)) == null) {
									throw new RuntimeException(String.format("Entity with id %s not found", entityConfig.getId(resourceInstance)));
								}
								dataAccessObject.delete(entityConfig.getId(resourceInstance));
								//hive.directory().deleteResourceId(resource.getName(), entityConfig.getId(resourceInstance));
							} catch (Exception e) { throw new RuntimeException(e); }
							assertFalse(hive.directory().doesResourceIdExist(resource.getName(), entityConfig.getId(resourceInstance)));
							
							Collection<? extends EntityIndexConfig> secondaryIndexConfigs = Filter.grep(new Predicate<EntityIndexConfig>() {
								public boolean f(EntityIndexConfig entityIndexConfig) {
									return !entityIndexConfig.getIndexType().equals(IndexType.Delegates);
								}}, getHiveIndexes());
							for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
								final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
								Collection<Object> secondaryIndexKeys = secondaryIndexConfig.getIndexValues(resourceInstance);
								for (Object secondaryIndexKey : secondaryIndexKeys)
										assertFalse(Filter.grepItemAgainstList(
												entityConfig.getId(resourceInstance),
												hive.directory().getResourceIdsOfSecondaryIndexKey(secondaryIndex.getResource().getName(), secondaryIndex.getName(), secondaryIndexKey)));													
							}
							
							new Undo() { public void f() {
								try {
									dataAccessObject.save(resourceInstance);
									//hive.directory().insertResourceId(resource.getName(), entityConfig.getId(resourceInstance), entityConfig.getPrimaryIndexKey(resourceInstance));
								} catch (Exception e) { throw new RuntimeException(e); }
								assertTrue(hive.directory().doesResourceIdExist(resource.getName(), entityConfig.getId(resourceInstance)));
							}};
					}}.cycle();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		
		}
		private void validateDeleteSecondaryIndexKeys(final EntityHiveConfig entityHiveConfig, final Hive hive, Class representedInterface, final Collection<Object> resourceInstances) {
			final EntityConfig entityConfig = entityHiveConfig.getEntityConfig(representedInterface);
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			
			for (final Object resourceInstance : resourceInstances) {
				// Test delete of secondary index keys individually
				Collection<? extends EntityIndexConfig> secondaryIndexConfigs = Filter.grep(new Predicate<EntityIndexConfig>() {
					public boolean f(EntityIndexConfig entityIndexConfig) {
						return !entityIndexConfig.getIndexType().equals(IndexType.Delegates);
					}}, getHiveIndexes());
				for (final EntityIndexConfig secondaryIndexConfig : secondaryIndexConfigs) {	
					final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());
					
					new Undoable() { 
						public void f() {
							final Object resourceId = entityConfig.getId(resourceInstance);
							final Collection<Object> secondaryIndexKeys = secondaryIndexConfig.getIndexValues(resourceInstance);
							for (final Object secondaryIndexKey : secondaryIndexKeys) {
								try {									
									hive.directory().deleteSecondaryIndexKey(
										resource.getName(),
										secondaryIndex.getName(),
										secondaryIndexKey,
										resourceId);
									assertFalse(Filter.grepItemAgainstList(resourceId,
												hive.directory().getResourceIdsOfSecondaryIndexKey(secondaryIndex.getResource().getName(), secondaryIndex.getName(), secondaryIndexKey)));
								} catch (Exception e) { throw new RuntimeException(e); }
								new Undo() { public void f() {
									try {
										hive.directory().insertSecondaryIndexKey(
												resource.getName(),
												secondaryIndex.getName(),
												secondaryIndexKey,
												resourceId
										);
									} catch (Exception e) { throw new RuntimeException(e); }
									Collection<Object> resourceIdsOfSecondaryIndexKey = hive.directory().getResourceIdsOfSecondaryIndexKey(secondaryIndex.getResource().getName(), secondaryIndex.getName(), secondaryIndexKey);
									assertTrue(Filter.grepItemAgainstList(
												resourceId,
												resourceIdsOfSecondaryIndexKey));
								}};						
							}
					}}.cycle(); 
				}
			}
		}
		
		private void undoSecondaryIndexDelete(
				final Hive hive,
				final EntityConfig entityConfig, 
				final EntityIndexConfig secondaryIndexConfig,
				final Object resourceInstance) {
			final PartitionDimension partitionDimension = hive.getPartitionDimension();
			final Resource resource = partitionDimension.getResource(entityConfig.getResourceName());
			final SecondaryIndex secondaryIndex = resource.getSecondaryIndex(secondaryIndexConfig.getIndexName());																								
			final Object resourceId = entityConfig.getId(resourceInstance);
			final Object secondaryIndexValue = secondaryIndexConfig.getIndexValues(resourceInstance);
			new Actor<Object>(secondaryIndexValue) {
				public void f(Object secondaryIndexKey) {
					try {	
						hive.directory().insertSecondaryIndexKey(
								resource.getName(), 
								secondaryIndex.getName(),
								secondaryIndexKey, 
								resourceId);
					} catch (Exception e) { 
						throw new RuntimeException(String.format("Failed to insert into %s id: %s pkey: %s", secondaryIndex.getName(),secondaryIndexKey,resourceId),e); 
					}
				assertTrue(Filter.grepItemAgainstList(
						resourceId,
						hive.directory().getResourceIdsOfSecondaryIndexKey(secondaryIndex.getResource().getName(), secondaryIndex.getName(), secondaryIndexKey)));
			}}.perform();
		};	
		
		
	}
	private static void assertEquality(final Hive hive, final PartitionDimension partitionDimension) {
		assertEquals(
			partitionDimension,
			hive.getPartitionDimension());
	}
	private static void assertEquality(final Hive hive, final Resource resource) {
		final Resource actual = hive.getPartitionDimension().getResource(resource.getName());
		assertEquals(
			resource,
			actual);
	}
	private static void assertEquality(final Hive hive, final Resource resource, final SecondaryIndex secondaryIndex)  {
		assertEquals(
			secondaryIndex,
			hive.getPartitionDimension().getResource(resource.getName()).getSecondaryIndex(secondaryIndex.getName()));
	}
	private static void assertEquality(final Hive hive, final Node node)  {
		assertEquals(
			node,
			hive.getNode(node.getId()));
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
	protected Hive getHive() {
		return hive;
	}
	public EntityHiveConfig getEntityHiveConfig() {
		return entityHiveConfig;
	}
	protected Class getRepresentedInterface() {
		return representedInterface;
	}
}
