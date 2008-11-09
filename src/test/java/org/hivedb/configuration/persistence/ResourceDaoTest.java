package org.hivedb.configuration.persistence;

import org.hivedb.Resource;
import org.hivedb.ResourceImpl;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.util.database.DatabaseAdapter;
import org.hivedb.util.database.DatabaseInitializer;
import org.hivedb.util.database.H2Adapter;
import org.hivedb.util.functional.Atom;
import static org.junit.Assert.assertEquals;
import org.junit.*;

import java.sql.Types;
import java.util.Collection;

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
  public void shouldLoadResources() throws Exception {
    ResourceDao d = new ResourceDao(adapter.getDataSource(DB_NAME));

    Resource resource = new ResourceImpl("aResource", Types.INTEGER, false);
    d.create(resource);

    Collection<Resource> resources = d.loadAll();
    assertEquals(1, resources.size());
    assertEquals(resource, Atom.getFirst(resources));
  }

//  @Test
//  public void testCreate() throws Exception {
//    DataSource mockDataSource = context.mock(DataSource.class);
//    ResourceDao d = new ResourceDao(mockDataSource);
//    Resource mockResource = context.mock(Resource.class);
//    d.create(mockResource);
//  }
}
