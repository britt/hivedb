package org.hivedb.util.database;

import org.hivedb.HiveRuntimeException;
import org.hivedb.persistence.DataSourceProvider;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.*;

@RunWith(JMock.class)
public class H2AdapterTest {
  private Mockery context;
  private H2Adapter adapter;
  private DataSourceProvider provider;

  @Before
  public void setUp() throws Exception {
    context = new JUnit4Mockery() {
      {
        //setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    provider = context.mock(DataSourceProvider.class);
    adapter = new H2Adapter(provider);
  }

  @Test
  public void shouldInitializeDriver() throws Exception {
    //Ensure driver has not already been initialized
    try {
      Driver driver = DriverManager.getDriver("jdbc:h2:mem:db;LOCK_MODE=3");
      if(driver != null) {
        DriverManager.deregisterDriver(driver);
      }
    } catch(SQLException e) {
      //quash exception thrown if the driver is not registered
    }
    adapter.initializeDriver();
    assertTrue(DriverManager.getDriver("jdbc:h2:mem:db;LOCK_MODE=3").getClass().isAssignableFrom(org.h2.Driver.class));
  }

  @Test
  public void shouldFormatConnectStrings() throws Exception {
    String connectString = adapter.getConnectString("db");
    assertEquals(HiveDbDialect.H2, DriverLoader.discernDialect(connectString));
  }
  
  @Test
  public void shouldGetConnectionsToAnH2Database() throws Exception {
    final DataSource mockDataSource = context.mock(DataSource.class);
    final Connection mockConnection = context.mock(Connection.class);
    mockConnectionEstablished(mockDataSource, mockConnection);

    assertEquals(mockConnection, adapter.getConnection("db"));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfAConnectionCannotBeMade() throws Exception {
    final DataSource mockDataSource = context.mock(DataSource.class);
    mockConnectionFailed(mockDataSource);
    
    adapter.getConnection("db");
  }



  @Test
  public void shouldGetADataSource() throws Exception {
    final DataSource mockDataSource = context.mock(DataSource.class);
    context.checking(new Expectations() {
      {
    	  one(provider).getDataSource(adapter.getConnectString("db"));
    		will(returnValue(mockDataSource));
      }
    });
    assertEquals(mockDataSource, adapter.getDataSource("db"));
  }
  
  @Test
  public void shouldCreateAnH2Database() throws Exception {
    final Connection mockConnection = context.mock(Connection.class);
    final DataSource mockDataSource= context.mock(DataSource.class);
    mockConnectionEstablished(mockDataSource, mockConnection);
    mockConnectionClosed(mockConnection);

    String dbName = "db";
    adapter.createDatabase(dbName);
    assertTrue(adapter.databaseExists(dbName));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfDbCreationFails() throws Exception {
    final Connection mockConnection = context.mock(Connection.class);
    final DataSource mockDataSource= context.mock(DataSource.class);
    mockConnectionEstablished(mockDataSource, mockConnection);
    mockConnectionCloseFails(mockConnection);

    String dbName = "db";
    adapter.createDatabase(dbName);
    assertFalse(adapter.databaseExists(dbName));
  }

  @Test
  public void shouldDropADatabase() throws Exception {
    final Connection mockConnection = context.mock(Connection.class);
    final DataSource mockDataSource = context.mock(DataSource.class);
    final Statement mockStatement = context.mock(Statement.class);

    mockConnectionEstablished(mockDataSource, mockConnection);
    mockShutdownStatement(mockConnection, mockStatement);

    String dbName = "db";
    adapter.dropDatabase(dbName);
    assertFalse(adapter.databaseExists(dbName));
  }

  @Test(expected = HiveRuntimeException.class)
  public void shouldThrowIfTheDropStatementFails() throws Exception {
    final Connection mockConnection = context.mock(Connection.class);
    final DataSource mockDataSource = context.mock(DataSource.class);
    final Statement mockStatement = context.mock(Statement.class);

    mockConnectionEstablished(mockDataSource, mockConnection);
    mockShutdownStatementFails(mockConnection, mockStatement);

    String dbName = "db";
    adapter.dropDatabase(dbName);
    assertFalse(adapter.databaseExists(dbName));
  }

  private void mockShutdownStatementFails(final Connection mockConnection, final Statement mockStatement) throws SQLException {
    context.checking(new Expectations() {
      {
        one(mockConnection).createStatement();
        will(returnValue(mockStatement));
        one(mockStatement).execute("SHUTDOWN");
        will(throwException(new SQLException()));
      }
    });
  }

  private void mockShutdownStatement(final Connection mockConnection, final Statement mockStatement) throws SQLException {
    context.checking(new Expectations() {
      {
        one(mockConnection).createStatement();
        will(returnValue(mockStatement));
        one(mockStatement).execute("SHUTDOWN");
        will(returnValue(true));
      }
    });
  }

  private void mockConnectionEstablished(final DataSource mockDataSource, final Connection mockConnection) throws SQLException {
    context.checking(new Expectations() {
      {
    	  one(provider).getDataSource(adapter.getConnectString("db"));
    		will(returnValue(mockDataSource));
        one(mockDataSource).getConnection();
        will(returnValue(mockConnection));
      }
    });
  }

  private void mockConnectionFailed(final DataSource mockDataSource) throws SQLException {
    context.checking(new Expectations() {
      {
    	  one(provider).getDataSource(adapter.getConnectString("db"));
    		will(returnValue(mockDataSource));
        one(mockDataSource).getConnection();
        will(throwException(new SQLException()));
      }
    });
  }

  private void mockConnectionClosed(final Connection mockConnection) throws SQLException {
    context.checking(new Expectations() {
      {
        one(mockConnection).close();
      }
    });
  }

  private void mockConnectionCloseFails(final Connection mockConnection) throws SQLException {
    context.checking(new Expectations() {
      {
        one(mockConnection).close();
        will(throwException(new SQLException()));
      }
    });
  }
}
