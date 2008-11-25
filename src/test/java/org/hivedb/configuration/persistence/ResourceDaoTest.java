package org.hivedb.configuration.persistence;

import org.hivedb.Resource;
import org.hivedb.ResourceImpl;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.util.database.DatabaseAdapter;
import org.hivedb.util.database.DatabaseInitializer;
import org.hivedb.util.database.H2Adapter;
import org.hivedb.util.functional.Atom;
import org.junit.*;
import static org.junit.Assert.assertEquals;

import java.sql.Types;
import java.util.Collection;

//Exception cases left untested because the base class that ResourceDao extends
//org.springframework.jdbc.core.support.JdbcDaoSupport contains final methods
//that cannot be overriden for the purposes of testing i.e. generating exceptions
//to trigger the exception handling code.
//
//TODO Refactor ResourceDao away from JdbcDaoSupport
public class ResourceDaoTest {
  private static DatabaseAdapter adapter;
  private static DatabaseInitializer db;
  private static final String DB_NAME = "hive";

  @BeforeClass
  public static void init() throws Exception {
    adapter = new H2Adapter(CachingDataSourceProvider.getInstance());
    adapter.initializeDriver();
    db = new DatabaseInitializer(adapter);
    db.addDatabase(DB_NAME, new HiveConfigurationSchema(adapter.getConnectString(DB_NAME)));
  }

  @Before
  public void setup() {
    db.initializeDatabases();
  }
  
  @After
  public void reset() {
    db.clearData();
  }

  @AfterClass
  public static void tearDown() {
    db.destroyDatabases();
  }
  
  @Test
  public void shouldCreateAndLoadResources() throws Exception {
    ResourceDao d = new ResourceDao(adapter.getDataSource(DB_NAME));

    Resource resource = new ResourceImpl("aResource", Types.INTEGER, false);
    d.create(resource);

    Collection<Resource> resources = d.loadAll();
    assertEquals(1, resources.size());
    assertEquals(resource, Atom.getFirst(resources));
  }

//  @Test
//  public void createShouldThrowIfTheRowIsNotInserted() throws Exception {
//    throw new UnsupportedOperationException("Not yet implemented");
//  }
//
//  @Test
//  public void createShouldThrowIfTheIdIsNotAssigned() throws Exception {
//    throw new UnsupportedOperationException("Not yet implemented");
//  }

  @Test
  public void shouldUpdateAResource() throws Exception {
    ResourceDao d = new ResourceDao(adapter.getDataSource(DB_NAME));

    Resource resource = new ResourceImpl("aResource", Types.INTEGER, false);
    d.create(resource);
    resource.setPartitioningResource(true);
    d.update(resource);
    Collection<Resource> resources = d.loadAll();
    assertEquals(1, resources.size());
    assertEquals(resource, Atom.getFirst(resources));
  }

//  @Test(expected = HiveRuntimeException.class)
//  public void shouldThrowIfMoreThanOneRowIsUpdated() throws Exception {
//    throw new UnsupportedOperationException("Not yet implemented");
//  }

  @Test
  public void shouldDeleteAResource() throws Exception {
    ResourceDao d = new ResourceDao(adapter.getDataSource(DB_NAME));

    Resource resource = new ResourceImpl("aResource", Types.INTEGER, false);
    d.create(resource);
    d.delete(resource);
    Collection<Resource> resources = d.loadAll();
    assertEquals(0, resources.size());
  }

//  @Test
//  public void shouldThrowIfMoreThanOneResourceIsDeleted() throws Exception {
//    throw new UnsupportedOperationException("Not yet implemented");
//  }
}
