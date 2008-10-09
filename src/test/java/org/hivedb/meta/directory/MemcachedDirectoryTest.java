package org.hivedb.meta.directory;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.hivedb.Lockable;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
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
import java.util.Collection;

@RunWith(JMock.class)
public class MemcachedDirectoryTest {
  private Mockery mockery;
  private PartitionDimension partitionDimension;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    Resource resource = new Resource("res", Types.INTEGER, false);
    Collection<Resource> resources = Lists.newList(resource);
    partitionDimension = new PartitionDimension("x", Types.INTEGER, resources);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIllegalStateExceptionIfSockIOPoolIsNotInitialized() throws Exception {
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    MemcachedDirectory directory = new MemcachedDirectory("uninitializedPool", client, keyBuilder, partitionDimension);
  }

  @Test
  public void shouldTestTheExistenceOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);

    final String key = mockPrimaryKeyExists(client, keyBuilder, true);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertTrue(directory.doesPrimaryIndexKeyExist(key));
  }

  @Test
  public void shouldTestTheNonExistenceOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);

    final String key = mockPrimaryKeyExists(client, keyBuilder, false);

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
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
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    final Object resourceId = new Integer(99);
    final MemcachedDirectory.ResourceCacheEntry entry = new MemcachedDirectory.ResourceCacheEntry("P", "R");

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));
        one(client).get("R");
        will(returnValue("PR"));
        one(client).get("PR");
        will(returnValue(entry));
        one(client).keyExists(entry.getPrimaryIndexCacheKey());
        will(returnValue(true));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertTrue(directory.doesResourceIdExist(resource, resourceId));
  }

  @Test
  public void shouldTestTheNonExistenceOfResourceIds() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    final Object resourceId = new Integer(99);
    final MemcachedDirectory.ResourceCacheEntry entry = new MemcachedDirectory.ResourceCacheEntry("P", "R");

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));
        one(client).get("R");
        will(returnValue("PR"));
        one(client).get("PR");
        will(returnValue(entry));
        one(client).keyExists(entry.getPrimaryIndexCacheKey());
        will(returnValue(false));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertFalse(directory.doesResourceIdExist(resource, resourceId));
  }

  private Pair<Resource, String> mockResourceIdExists(final MemCachedClient client, final CacheKeyBuilder keyBuilder, final boolean exists) {
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
  public void shouldGetKeySemaphoresOfPrimaryKeys() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, 4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));
        one(client).get("P");
        will(returnValue(semaphore));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertEquals(1, directory.getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey).size());
  }

  @Test
  public void shouldReturnAnEmptyCollectionWhenPrimaryKeyIsNotFound() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));
        one(client).get("P");
        will(returnValue(null));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertTrue(directory.getKeySemamphoresOfPrimaryIndexKey(primaryIndexKey).isEmpty());
  }

  @Test
  public void shouldInsertPrimaryIndexKeySemaphore() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Node node = new Node();
    node.setId(4);
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, node.getId());

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));
        one(client).keyExists("P");
        will(returnValue(false));
        one(keyBuilder).buildCounterKey(primaryIndexKey, "res");
        will(returnValue("PRC"));
        one(client).set("P", semaphore);
        one(client).storeCounter("PRC", 0);
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.insertPrimaryIndexKey(node, primaryIndexKey);
  }


  @Test(expected = IllegalStateException.class)
  public void shouldThrowAnIllegalStateExceptionIfThePrimaryIndexKeyAlreadyExists() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Node node = new Node();
    node.setId(4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));
        one(client).keyExists("P");
        will(returnValue(true));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.insertPrimaryIndexKey(node, primaryIndexKey);
  }

  @Test
  public void shouldDeleteAllKeysOnPrimaryKeyDelete() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Node node = new Node();
    node.setId(4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));
        one(keyBuilder).buildCounterKey(primaryIndexKey, "res");
        will(returnValue("PRC"));

        one(client).getCounter("PRC");
        will(returnValue(1L));

        one(keyBuilder).buildReferenceKey(primaryIndexKey, "res", 0L);
        will(returnValue("PR"));

        one(client).get("PR");
        will(returnValue(new MemcachedDirectory.ResourceCacheEntry("P", "R")));

        one(client).delete("P");
        one(client).delete("PRC");
        one(client).delete("R");
        one(client).delete("PR");

      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.deletePrimaryIndexKey(primaryIndexKey);
  }

  @Test
  public void shouldGetKeySemaphoresOfResourceId() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Integer resourceId = new Integer(2);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    final Node node = new Node();
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, 4);
    node.setId(4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));

        one(client).get("R");
        will(returnValue("PR"));

        one(client).get("PR");
        will(returnValue(new MemcachedDirectory.ResourceCacheEntry("P", "R")));

        one(client).get("P");
        will(returnValue(semaphore));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    Collection<KeySemaphore> semaphores = directory.getKeySemaphoresOfResourceId(resource, resourceId);
    assertEquals(1, semaphores.size());
    assertTrue(semaphores.contains(semaphore));
  }


  @Test
  public void shouldGetPrimaryIndexKeyOfResourceId() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Integer resourceId = new Integer(2);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    int nodeId = 4;
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, nodeId);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));

        one(client).get("R");
        will(returnValue("PR"));

        one(client).get("PR");
        will(returnValue(new MemcachedDirectory.ResourceCacheEntry("P", "R")));

        one(client).get("P");
        will(returnValue(semaphore));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    assertEquals(primaryIndexKey, directory.getPrimaryIndexKeyOfResourceId(resource, resourceId));
  }

  @Test
  public void shouldUpdatePrimaryIndexKeyReadOnly() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    int nodeId = 4;
    final KeySemaphoreImpl semaphore = new KeySemaphoreImpl(primaryIndexKey, nodeId);
    semaphore.setStatus(Lockable.Status.writable);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));

        one(client).get("P");
        will(returnValue(semaphore));

        one(client).set("P", semaphore);
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.updatePrimaryIndexKeyReadOnly(primaryIndexKey, true);
    assertEquals(Lockable.Status.readOnly, semaphore.getStatus());
  }

  @Test
  public void shouldUpdatePrimaryIndexKeyWriteable() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    int nodeId = 4;
    final KeySemaphoreImpl semaphore = new KeySemaphoreImpl(primaryIndexKey, nodeId);
    semaphore.setStatus(Lockable.Status.readOnly);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));

        one(client).get("P");
        will(returnValue(semaphore));

        one(client).set("P", semaphore);
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.updatePrimaryIndexKeyReadOnly(primaryIndexKey, false);
    assertEquals(Lockable.Status.writable, semaphore.getStatus());
  }

  @Test
  public void shouldInsertResourceId() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Integer resourceId = new Integer(2);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, 4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).buildCounterKey(primaryIndexKey, resource.getName());
        will(returnValue("PRC"));

        one(client).addOrIncr("PRC");
        will(returnValue(4L));

        one(keyBuilder).buildReferenceKey(primaryIndexKey, resource.getName(), 4L);
        will(returnValue("PR"));

        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));

        one(client).set("R", "PR");

        one(keyBuilder).build(primaryIndexKey);
        will(returnValue("P"));

        one(client).set(with(equal("PR")), with(equal(new MemcachedDirectory.ResourceCacheEntry("P", "R"))));
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.insertResourceId(resource, resourceId, primaryIndexKey);
  }

  @Test
  public void shouldDeleteResourceId() throws Exception {
    String poolName = getInitializedPool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class);
    final CacheKeyBuilder keyBuilder = mockery.mock(CacheKeyBuilder.class);
    final Integer primaryIndexKey = new Integer(1);
    final Integer resourceId = new Integer(2);
    final Resource resource = new Resource("res", Types.INTEGER, false);
    final KeySemaphore semaphore = new KeySemaphoreImpl(primaryIndexKey, 4);

    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(resource.getName(), resourceId);
        will(returnValue("R"));

        one(client).get("R");
        will(returnValue("PR"));

        one(client).delete("PR");
        one(client).delete("R");
      }
    });

    MemcachedDirectory directory = new MemcachedDirectory(poolName, client, keyBuilder, partitionDimension);
    directory.deleteResourceId(resource, resourceId);
  }


  private String getInitializedPool
      () {
    String name = getClass().getName() + System.currentTimeMillis();
    SockIOPool sockIOPool = SockIOPool.getInstance(name);
    sockIOPool.setServers(new String[]{"127.0.0.1:11211"});
    sockIOPool.initialize();
    return name;
  }
}