package org.hivedb.persistence;

import org.hivedb.Resource;
import org.hivedb.ResourceImpl;
import org.hivedb.SecondaryIndex;
import org.hivedb.SecondaryIndexImpl;
import org.hivedb.configuration.persistence.HiveConfigurationSchema;
import org.hivedb.configuration.persistence.SecondaryIndexDao;
import org.hivedb.util.Lists;
import org.hivedb.util.database.DatabaseAdapter;
import org.hivedb.util.database.DatabaseInitializer;
import org.hivedb.util.database.H2Adapter;
import org.hivedb.util.functional.Atom;
import org.junit.*;
import static org.junit.Assert.assertEquals;

import java.sql.Types;

public class TestSecondaryIndexPersistence {
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
  public void shouldCreateAndLoadASecondaryIndex() throws Exception {
    SecondaryIndexDao d = new SecondaryIndexDao(adapter.getDataSource(DB_NAME));

    SecondaryIndex secondaryIndex = new SecondaryIndexImpl("anIndex", Types.INTEGER);
    Resource resource = new ResourceImpl(1,"aResource", Types.INTEGER, false, Lists.newList(secondaryIndex));
    secondaryIndex.setResource(resource);
    d.create(secondaryIndex);
    SecondaryIndex fetched = Atom.getFirst(d.loadAll());
    assertEquals(secondaryIndex, fetched);
  }

  @Test
  public void shouldUpdateASecondaryIndex() throws Exception {
    SecondaryIndexDao d = new SecondaryIndexDao(adapter.getDataSource(DB_NAME));

    SecondaryIndexImpl secondaryIndex = new SecondaryIndexImpl("anIndex", Types.INTEGER);
    Resource resource = new ResourceImpl(1,"aResource", Types.INTEGER, false, Lists.newList(new SecondaryIndex[]{secondaryIndex}));
    secondaryIndex.setResource(resource);
    d.create(secondaryIndex);
    secondaryIndex.setColumnInfo(new ColumnInfo("column", Types.BIGINT));
    d.update(secondaryIndex);
    SecondaryIndex fetched = Atom.getFirst(d.findByResource(resource.getId()));
    assertEquals(secondaryIndex, fetched);
  }

  @Test
  public void shouldDeleteSecondaryIndex() throws Exception {
    SecondaryIndexDao d = new SecondaryIndexDao(adapter.getDataSource(DB_NAME));

    SecondaryIndexImpl secondaryIndex = new SecondaryIndexImpl("anIndex", Types.INTEGER);
    Resource resource = new ResourceImpl(1,"aResource", Types.INTEGER, false, Lists.newList(new SecondaryIndex[]{secondaryIndex}));
    secondaryIndex.setResource(resource);
    d.create(secondaryIndex);
    d.delete(secondaryIndex);
    assertEquals(0, d.loadAll().size());
  }

//	@Test
//	public void testCreate() throws Exception {
//		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getConnectString(getHiveDatabaseName())));
//		int initialSize = d.loadAll().size();
//		d.create(createSecondaryIndex());
//		assertEquals(initialSize+1,d.loadAll().size());
//	}
//
//	@Test
//	public void testDelete() throws Exception {
//		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getConnectString(getHiveDatabaseName())));
//		int initialSize = d.loadAll().size();
//		int id = d.create(createSecondaryIndex());
//		assertEquals(initialSize+1,d.loadAll().size());
//		SecondaryIndex s = createSecondaryIndex(id);
//		s.setResource(createResource());
//		d.delete(s);
//		assertEquals(initialSize,d.loadAll().size());
//	}
}
