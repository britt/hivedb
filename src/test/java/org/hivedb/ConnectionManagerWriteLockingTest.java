package org.hivedb;

import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.directory.DirectoryFacade;
import org.hivedb.directory.KeySemaphoreImpl;
import org.hivedb.persistence.HiveDataSourceProvider;
import org.hivedb.util.Lists;
import org.hivedb.util.database.HiveDbDialect;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@RunWith(JMock.class)
public class ConnectionManagerWriteLockingTest {
  private Mockery context;
  private HiveConfiguration hiveConfiguration;
  private DirectoryFacade directory;
  private ConnectionManager connection;
  private HiveDataSourceProvider datasources;
  private DataSource datasource;


  @Before
  public void setup() throws Exception {
    context = new JUnit4Mockery() {
      {
//        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    hiveConfiguration = context.mock(HiveConfiguration.class);
    directory = context.mock(DirectoryFacade.class);
    datasources = context.mock(HiveDataSourceProvider.class);
    datasource = mockConnectionmanagerInitialized();

    connection = new ConnectionManager(directory, hiveConfiguration, datasources);
  }

  @Test(expected = HiveLockableException.class)
  public void shouldNotIssueReadWriteConnectionsWhenHiveIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.writable);
    mockGetSemaphore(Lockable.Status.readOnly);
    mockGetNodes(Lockable.Status.writable);

    connection.getByPartitionKey(key, AccessType.ReadWrite);
  }

  @Test
  public void shouldIssueReadConnectionsWhenTheHiveIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.writable);
    mockGetConnection();

    assertNotNull(connection.getByPartitionKey(key, AccessType.Read));
  }

  @Test(expected = HiveLockableException.class)
  public void shouldNotIssueReadWriteConnectionsWhenNodeIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.writable);
    mockGetSemaphore(Lockable.Status.writable);
    mockGetNodes(Lockable.Status.readOnly);

    connection.getByPartitionKey(key, AccessType.ReadWrite);
  }

  @Test
  public void shouldIssueReadConnectionsWhenTheNodeIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.writable);
    mockGetConnection();

    assertNotNull(connection.getByPartitionKey(key, AccessType.Read));
  }

  @Test(expected = HiveLockableException.class)
  public void shouldNotIssueReadWriteConnectionsWhenPartitionKeyIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.readOnly);
    mockGetSemaphore(Lockable.Status.writable);
    mockGetNodes(Lockable.Status.writable);

    connection.getByPartitionKey(key, AccessType.ReadWrite);
  }

  @Test
  public void shouldIssueReadConnectionsWhenThePartitionKeyIsWriteLocked() throws Exception {
    final String key = new String("North America");
    mockPartitionKeyFound(key, Lockable.Status.readOnly);
    mockGetConnection();

    assertNotNull(connection.getByPartitionKey(key, AccessType.Read));
  }

  private DataSource mockConnectionmanagerInitialized() {
    final DataSource datasource = context.mock(DataSource.class);
    context.checking(new Expectations() {
      {
        exactly(2).of(hiveConfiguration).getNodes();
        Node node = getNode(Lockable.Status.writable);
        will(returnValue(Lists.newList(node)));
        exactly(2).of(datasources).getDataSource(node.getUri());
        will(returnValue(datasource));
      }
    });
    return datasource;
  }

  private void mockPartitionKeyFound(final Object key, final Lockable.Status status) {
    context.checking(new Expectations() {
      {
        one(directory).getKeySemamphoresOfPartitionKey(key);
        will(returnValue(Lists.newList(new KeySemaphoreImpl(key, 1, status))));
      }
    });
  }

  private void mockGetSemaphore(final Lockable.Status status) {
    context.checking(new Expectations() {
      {
        one(hiveConfiguration).getSemaphore();
        will(returnValue(new HiveSemaphoreImpl(status, 1)));
      }
    });
  }
  
  private void mockGetConnection() throws SQLException {
    context.checking(new Expectations() {
      {
        one(datasource).getConnection();
        will(returnValue(context.mock(Connection.class)));
      }
    });
  }

  private void mockGetNodes(final Lockable.Status status) {
    context.checking(new Expectations() {
      {
        exactly(1).of(hiveConfiguration).getNodes();
        will(returnValue(Lists.newList(getNode(status))));
      }
    });
  }
  
  private Node getNode(Lockable.Status status) {
    Node node = new Node(1, "node", "db", "host", HiveDbDialect.H2);
    node.setStatus(status);
    return node;
  }
}
