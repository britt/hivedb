package org.hivedb.util.database;

import org.hivedb.persistence.Schema;
import org.hivedb.util.Lists;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class DatabaseInitializerTest {
  private Mockery context;
  private DatabaseInitializer initializer;
  private DatabaseAdapter adapter;
  private static final String DB_NAME = "myDb";

  @Before
  public void setUp() throws Exception {
    context = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };

    adapter = context.mock(DatabaseAdapter.class);
    initializer = new DatabaseInitializer(adapter);
  }

  @Test
  public void initializeShouldCreateDatabases() throws Exception {
    final Schema mockSchema = context.mock(Schema.class);
    mockDatabaseExists(false);
    context.checking(new Expectations() {
      {
        one(adapter).createDatabase(DB_NAME);
      }
    });
    mockSchemasInstalled(mockSchema);

    initializer.addDatabase(DB_NAME, mockSchema);
    initializer.initializeDatabases();
  }

  @Test
  public void shouldNotCreateDatabasesIfTheyAlreadyExist() throws Exception {
    final Schema mockSchema = context.mock(Schema.class);
    mockDatabaseExists(true);
    mockSchemasInstalled(mockSchema);

    initializer.addDatabase(DB_NAME, mockSchema);
    initializer.initializeDatabases();
  }

  @Test
  public void shouldDestoryDatabases() throws Exception {
    mockDatabaseExists(true);
    context.checking(new Expectations() {
      {
        one(adapter).dropDatabase(DB_NAME);
      }
    });
    final Schema mockSchema = context.mock(Schema.class);

    initializer.addDatabase(DB_NAME, mockSchema);
    initializer.destroyDatabases();
  }

  private void mockDatabaseExists(final boolean exists) {
    context.checking(new Expectations() {
      {
        one(adapter).databaseExists(DB_NAME);
        will(returnValue(exists));
      }
    });
  }

  private void mockSchemasInstalled(final Schema mockSchema) {
    context.checking(new Expectations() {
      {
        one(adapter).getConnectString(DB_NAME);
        String uri = "connect string";
        will(returnValue(uri));
        one(mockSchema).getTables(uri);
        will(returnValue(Lists.newArrayList()));
      }
    });
  }
}
