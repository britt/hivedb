package org.hivedb.meta.directory;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.hivedb.Lockable;
import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Pair;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Types;

@RunWith(JMock.class)
public class MemcachedDirectoryTest {
  private Mockery mockery;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIllegalStateExceptionIfSockIOPoolIsNotInitialized() throws Exception {
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);
    MemcachedDirectory directory = new MemcachedDirectory("uninitializedPool", client, keyBuilder);
  }

  @Test
  public void shouldTestTheExistenceOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final String key = mockPrimaryKeyExists(client, keyBuilder, true);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertTrue(directory.doesPrimaryIndexKeyExist(key));
  }

  @Test
  public void shouldTestTheNonExistenceOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final String key = mockPrimaryKeyExists(client, keyBuilder, false);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertFalse(directory.doesPrimaryIndexKeyExist(key));
  }

  private String mockPrimaryKeyExists(final MemCachedClient client, final CacheKeyBuilder keyBuilder, final boolean exists) {
    final String key = "aKey";
    final String cacheKey = "cacheKey";

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(key);
        will(returnValue(cacheKey));
        one(client).keyExists(cacheKey);
        will(returnValue(exists));

      }
    });
    return key;
  }

  @Test
  public void shouldTestTheExistenceOfResourceIds() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final Pair<Resource, String> pair = mockResourceIdExists(client, keyBuilder, true);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertTrue(directory.doesResourceIdExist(pair.getKey(), pair.getValue()));
  }


  @Test
  public void shouldTestTheNonExistenceOfResourceIds() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final Pair<Resource, String> pair = mockResourceIdExists(client, keyBuilder, false);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertFalse(directory.doesResourceIdExist(pair.getKey(), pair.getValue()));
  }

  private Pair<Resource, String> mockResourceIdExists(final MemCachedClient client, final MemcacheDirectoryKeyBuilder keyBuilder, final boolean exists) {
    final String key = "aKey";
    final String cacheKey = "cacheKey";
    final Resource resource = new Resource("resource", Types.INTEGER, false);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), key);
        will(returnValue(cacheKey));
        one(client).get(cacheKey);
        String referenceKey = cacheKey + 1;
        will(returnValue(referenceKey));
        one(client).keyExists(referenceKey);
        will(returnValue(exists));
      }
    });
    return new Pair<Resource, String>(resource, key);
  }

  @Test
  public void shouldTestTheExistenceOfSecondaryIndexKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final Object[] argz = mockSecondaryIndexKeyExists(client, keyBuilder, true);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertTrue(directory.doesSecondaryIndexKeyExist((SecondaryIndex) argz[0], argz[1], argz[2]));
  }


  @Test
  public void shouldTestTheNonExistenceOfSecondaryIndexKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final Object[] argz = mockSecondaryIndexKeyExists(client, keyBuilder, false);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertFalse(directory.doesSecondaryIndexKeyExist((SecondaryIndex) argz[0], argz[1], argz[2]));
  }

  private Object[] mockSecondaryIndexKeyExists(final MemCachedClient client, final MemcacheDirectoryKeyBuilder keyBuilder, final boolean exists) {
    final String cacheKey = "cacheKey";
    final Resource resource = new Resource("resource", Types.INTEGER, false);
    final SecondaryIndex secondaryIndex = new SecondaryIndex("index", 0);
    secondaryIndex.setResource(resource);
    final int secondaryIndexKey = 1;
    final int resourceId = 2;

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), secondaryIndex.getName(), secondaryIndexKey, resourceId);
        will(returnValue(cacheKey));
        one(client).get(cacheKey);
        String referenceKey = cacheKey + "-ref";
        will(returnValue(referenceKey));
        one(client).keyExists(referenceKey);
        will(returnValue(exists));
      }
    });
    return new Object[]{secondaryIndex, secondaryIndexKey, resourceId};
  }

  @Test
  public void shouldGetKeySemaphoresOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final String key = mockPrimaryIndexKeyFound(client, keyBuilder, Lists.newList(new KeySemaphoreImpl("", 0, Lockable.Status.writable)));

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertEquals(1, directory.getKeySemamphoresOfPrimaryIndexKey(key).size());
  }

  @Test
  public void shouldReturnAnEmptyCollectionWhenPrimaryKeyIsNotFound() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    final String key = mockPrimaryIndexKeyFound(client, keyBuilder, null);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    assertTrue(directory.getKeySemamphoresOfPrimaryIndexKey(key).isEmpty());
  }

  @Test
  public void shouldInsertPrimaryIndexKeySemaphore() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    String primaryIndexKey = mockPrimaryKeyExists(client, keyBuilder, false);
    mockPrimaryIndexKeyInserted(client, keyBuilder, primaryIndexKey);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    directory.insertPrimaryIndexKey(new Node(), primaryIndexKey);
  }

  private void mockPrimaryIndexKeyInserted(final MemCachedClient client, final MemcacheDirectoryKeyBuilder keyBuilder, final String primaryIndexKey) {
    final String cacheKey = "cacheKey";
    final String counterKey = "counterKey";
    final Node node = new Node();
    mockery.checking(new Expectations() {
    					{
    						one(keyBuilder).build(primaryIndexKey);
    						will(returnValue(cacheKey));
                one(client).add(cacheKey, new KeySemaphoreImpl(primaryIndexKey, node.getId(), Lockable.Status.writable));
                will(returnValue(true));
                one(keyBuilder).buildCounterKey(primaryIndexKey);
                will(returnValue(counterKey));
                one(client).addOrIncr(counterKey);
              }
    				});
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowAnIllegalStateExceptionIfThePrimaryIndexKeyALreadyExists() throws Exception {
      String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final MemcacheDirectoryKeyBuilder keyBuilder = mockery.mock(MemcacheDirectoryKeyBuilder.class);

    String primaryIndexKey = mockPrimaryKeyExists(client, keyBuilder, true);
    mockPrimaryIndexKeyInserted(client, keyBuilder, primaryIndexKey);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder);
    directory.insertPrimaryIndexKey(new Node(), primaryIndexKey);
  }

  private String mockPrimaryIndexKeyFound(final MemCachedClient client, final CacheKeyBuilder keyBuilder, final Object clientReturn) {
    final String key = "aKey";
    final String cacheKey = "cacheKey";

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(key);
        will(returnValue(cacheKey));
        one(client).get(cacheKey);
        will(returnValue(clientReturn));
      }
    });
    return key;
  }

  private String getInitializedPool() {
    String name = getClass().getName() + System.currentTimeMillis();
    SockIOPool sockIOPool = SockIOPool.getInstance(name);
    sockIOPool.setServers(new String[]{"127.0.0.1:11211"});
    sockIOPool.initialize();
    return name;
  }
}