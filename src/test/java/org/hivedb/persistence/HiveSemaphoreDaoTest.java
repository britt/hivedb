package org.hivedb.persistence;

import org.hivedb.HiveSemaphore;
import org.hivedb.HiveSemaphoreImpl;
import org.hivedb.Lockable;
import org.hivedb.configuration.persistence.HiveConfigurationSchema;
import org.hivedb.configuration.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.DatabaseInitializer;
import org.hivedb.util.database.H2Adapter;
import org.junit.*;
import static org.junit.Assert.assertEquals;

// TODO Complete exception cases
public class HiveSemaphoreDaoTest {
  private static final String DB_NAME = "hive";
  private static DatabaseInitializer db;
  private static H2Adapter adapter;

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
  public void shouldCreateAndFetchHiveSemaphore() throws Exception {
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(adapter.getDataSource(DB_NAME));
    HiveSemaphore hs = hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    assertEquals(hs, hsd.get());
  }

  @Test(expected = IllegalStateException.class)
  public void getShouldThrowIfTwoSemaphoresAreCreated() throws Exception {
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(adapter.getDataSource(DB_NAME));
    hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hsd.get();
  }

  @Test(expected = HiveSemaphoreDao.HiveSemaphoreNotFound.class)
  public void getShouldThrowIfNoSemaphoreExists() throws Exception {
    HiveSemaphoreDao hsd = new HiveSemaphoreDao(adapter.getDataSource(DB_NAME));
    hsd.get();
  }

  @Test
  public void testUpdate() throws Exception {

    HiveSemaphoreDao hsd = new HiveSemaphoreDao(adapter.getDataSource(DB_NAME));
    HiveSemaphore hs = hsd.create(new HiveSemaphoreImpl(Lockable.Status.writable, 1));
    hs.incrementRevision();
    hsd.update(hs);

    HiveSemaphore hs2 = hsd.get();
    Assert.assertEquals(hs.getRevision(), hs2.getRevision());
    Assert.assertEquals(hs.getStatus(), hs2.getStatus());
  }
}
