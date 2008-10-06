package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Hive;
import org.hivedb.meta.directory.KeySemaphoreImpl;
import org.hivedb.util.Lists;
import org.hivedb.util.cache.Cache;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

// TODO check assertion messages
@RunWith(JMock.class)
public class CachingDirectoryFacadeTest {
  private final static Log log = LogFactory.getLog(CachingDirectoryFacadeTest.class);

  private Mockery mockery;
  private DirectoryFacade delegate;
  private CacheKeyBuilder keyBuilder;
  private Cache cache;
  private String cacheKey;
  private Hive hive;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };

    delegate = mockery.mock(DirectoryFacade.class, "delegate");
    keyBuilder = mockery.mock(CacheKeyBuilder.class);
    cache = mockery.mock(Cache.class);
    hive = mockery.mock(Hive.class);
    cacheKey = "cacheKey";
  }

  @Test
  public void doesResourceIdExistShouldReturnTrueIfKeyInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatResourceKeyExistsInCache(resource, resourceId, true);

    assertTrue("key should exist", facade.doesResourceIdExist(resource, resourceId));
  }

  @Test
  public void doesResourceIdExistShouldReturnFalseIfKeyNotFoundInCacheAndDelegate() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatResourceKeyExistsInCache(resource, resourceId, false);
    mockResourceIdExistsInDelegate(resource, resourceId, false);
    
    assertFalse("key should not exist", facade.doesResourceIdExist(resource, resourceId));    
  }

  @Test
  public void doesResourceIdExistShouldReturnTrueIfKeyInDelegateAndNotInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatResourceKeyExistsInCache(resource, resourceId, false);
    mockResourceIdExistsInDelegate(resource, resourceId, true);
    
    assertTrue("key should exist", facade.doesResourceIdExist(resource, resourceId));
  }

  @Test
  public void doesResourceIdExistShouldDelegateIfCacheReadThrowsException() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatCacheKeyExistsForResourceIdThrowsException(resource, resourceId);
    mockResourceIdExistsInDelegate(resource, resourceId, true);

    assertTrue("key should exist", facade.doesResourceIdExist(resource, resourceId));
  }

  @Test
  public void getNodeIdsOfResourceIdShouldDelegateIfCacheReadThrwosAnException() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatResourceIdCacheReadThrowsException(resource, resourceId, cacheKey, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);
   
    assertSame("return value not from cache", expected, facade.getNodeIdsOfResourceId(resource, resourceId));
  }

  private void mockThatGetNodeIdsOfResourceIdReturnsValue(final String resource, final Object resourceId, final Collection<Integer> expected) {
    mockery.checking(new Expectations() {
      {
        one(delegate).getNodeIdsOfResourceId(resource, resourceId);
        will(returnValue(expected));
      }
    });
  }

  private void mockThatGetKeySemaphoresOfResourceIdReturnsValue(final String resource, final Object resourceId, final Collection<KeySemaphoreImpl> expected) {
    mockery.checking(new Expectations() {
      {
        one(delegate).getKeySemaphoresOfResourceId(resource, resourceId);
        will(returnValue(expected));
      }
    });
  }

  @Test
  public void getNodeIdsOfResourceIdShouldStillReturnIfCacheWriteFails() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsNotFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatCacheWriteThrowsException(cacheKey);

    assertSame("return value not from cache", expected, facade.getNodeIdsOfResourceId(resource, resourceId));
  }

  private void mockResourceIdIsNotFoundInCache(final String resource, final Object resourceId, final Collection expected, final CacheKeyBuilder.Mode mode) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(mode, resource, resourceId);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(returnValue(null));
      }
    });
  }

  private void mockResourceIdIsFoundInCache(final String resource, final Object resourceId, final Collection expected, final CacheKeyBuilder.Mode mode) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(mode, resource, resourceId);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(returnValue(expected));
      }
    });
  }

  @Test
  public void getNodeIdsOfResourceIdShouldNotDelegateIfExistsInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.key);

    Collection<Integer> actual = facade.getNodeIdsOfResourceId(resource, resourceId);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getNodeIdsOfResourceIdShouldDelegateIfDoesNotExistInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsNotFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);
    
    assertSame("return value not from cache", expected, facade.getNodeIdsOfResourceId(resource, resourceId));
  }



  @Test
  public void getKeySemaphoresOfResourceIdShouldDelegateIfCacheReadThrwosAnException() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatResourceIdCacheReadThrowsException(resource, resourceId, cacheKey, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    assertSame("return value not from delegate", expected, facade.getKeySemaphoresOfResourceId(resource, resourceId));
  }

  @Test
  public void getKeySemaphoresOfResourceIdShouldStillReturnIfCacheWriteFails() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsNotFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatCacheWriteThrowsException(cacheKey);

    assertSame("return value not from delegate", expected, facade.getKeySemaphoresOfResourceId(resource, resourceId));
  }

  @Test
  public void getKeySemaphoresOfResourceIdShouldNotDelegateIfExistsInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.semaphore);

    assertSame("return value not from cache", expected, facade.getKeySemaphoresOfResourceId(resource, resourceId));
  }

  @Test
  public void getKeySemaphoresOfResourceIdShouldDelegateIfDoesNotExistInCache() throws Exception {
    final String resource = "resource";
    final Object resourceId = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockResourceIdIsNotFoundInCache(resource, resourceId, expected, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfResourceIdReturnsValue(resource, resourceId, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);
    
    assertSame("return value not from delegate", expected, facade.getKeySemaphoresOfResourceId(resource, resourceId));
  }

  private void mockThatResourceIdCacheReadThrowsException(final String reosurce, final Object resourceId, final Object cacheKey, final CacheKeyBuilder.Mode mode) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(mode, reosurce, resourceId);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(throwException(new RuntimeException()));
      }
    });
  }

  private void mockThatResourceKeyExistsInCache(final String resource, final Object resourceId, final boolean exists) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(CacheKeyBuilder.Mode.key, resource, resourceId);
        will(returnValue(cacheKey));
        one(cache).exists(cacheKey);
        will(returnValue(exists));
      }
    });
  }

  private void mockResourceIdExistsInDelegate(final String resource, final Object resourceId, final boolean exists) {
    mockery.checking(new Expectations() {
      {
        one(delegate).doesResourceIdExist(resource, resourceId);
        will(returnValue(exists));
      }
    });
  }

  private void mockThatCacheKeyExistsForResourceIdThrowsException(final String resource, final Object resourceId) {
      mockery.checking(new Expectations() {
        {
          one(keyBuilder).build(CacheKeyBuilder.Mode.key, resource, resourceId);
          will(returnValue(cacheKey));
          one(cache).exists(cacheKey);
          will(throwException(new RuntimeException()));
        }
      });
    }

  @Test
  public void doesPrimaryIndexKeyExistShouldReturnTrueIfKeyInCache() throws Exception {
    final Object primaryKey = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryKeyExistsInCache(primaryKey, true);

    assertTrue("key should exist", facade.doesPrimaryIndexKeyExist(primaryKey));
  }

  @Test
  public void doesPrimaryIndexKeyExistShouldReturnFalseIfKeyNotInCacheAndNotFoundByDelegate() throws Exception {
    final Object primaryKey = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryKeyExistsInCache(primaryKey, false);
    mockPrimaryIndexKeyExistsInDelegate(primaryKey, false);

    assertFalse("key should not exist", facade.doesPrimaryIndexKeyExist(primaryKey));
  }

  @Test
  public void doesPrimaryIndexKeyExistShouldReturnTrueIfKeyNotInCacheAndFoundByDelegate() throws Exception {
    final Object primaryKey = new Object();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryKeyExistsInCache(primaryKey, false);
    mockPrimaryIndexKeyExistsInDelegate(primaryKey, true);

    assertTrue("key should exist", facade.doesPrimaryIndexKeyExist(primaryKey));
  }

  @Test
  public void doesPrimaryIndexKeyExistShouldDelegateIfCacheException() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatCacheKeyExistsForPrimaryIndexKeyThrowsException(primaryKey);
    mockPrimaryIndexKeyExistsInDelegate(primaryKey, true);

    assertTrue("key should exist", facade.doesPrimaryIndexKeyExist(primaryKey));
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldDelegateIfCacheThrowsExceptionOnRead() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryIndexKeyCacheReadThrowsException(primaryKey, cacheKey, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldStillReturnIfCacheWriteFails() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryIndexKeyIsNotFoundInCache(primaryKey, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatCacheWriteThrowsException(cacheKey);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldNotDelegateIfFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryIndexKeyIsFoundInCache(primaryKey, expected, CacheKeyBuilder.Mode.key);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldDelegateIfNotFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();

    mockThatPrimaryIndexKeyIsNotFoundInCache(primaryKey, CacheKeyBuilder.Mode.key);
    mockThatGetNodeIdsOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);
    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from delegate", expected, actual);
  }

  @Test
  public void getKeySemaphoresOfPrimaryIndexKeyshouldDelegateIfCacheThrowsExceptionOnRead() throws Exception {
    final Object primaryKey = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryIndexKeyCacheReadThrowsException(primaryKey, cacheKey, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    Collection<KeySemaphoreImpl> actual = facade.getKeySemamphoresOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getKeySemaphoresOfPrimaryIndexKeyshouldStillReturnIfCacheWriteFails() throws Exception {
    final Object primaryKey = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatPrimaryIndexKeyIsNotFoundInCache(primaryKey, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatCacheWriteThrowsException(cacheKey);

    Collection<KeySemaphoreImpl> actual = facade.getKeySemamphoresOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void geKeySemaphoresOfPrimaryIndexKeyshouldNotDelegateIfFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatKeySemaphoreOfPrimaryIndexKeyIsFoundInCache(primaryKey, expected);

    Collection<KeySemaphoreImpl> actual = facade.getKeySemamphoresOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  private void mockThatKeySemaphoreOfPrimaryIndexKeyIsFoundInCache(final Object primaryKey, final Collection<KeySemaphoreImpl> expected) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(CacheKeyBuilder.Mode.semaphore, primaryKey);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(returnValue(expected));
      }
    });
  }

  @Test
  public void getKeySemaphoresOfPrimaryIndexKeyshouldDelegateIfNotFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<KeySemaphoreImpl> expected = Lists.newArrayList();

    mockThatPrimaryIndexKeyIsNotFoundInCache(primaryKey, CacheKeyBuilder.Mode.semaphore);
    mockThatGetKeySemaphoresOfPrimaryIndexKeyReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);
    Collection<KeySemaphoreImpl> actual = facade.getKeySemamphoresOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from delegate", expected, actual);
  }

  private void mockThatGetKeySemaphoresOfPrimaryIndexKeyReturnsValue(final Object primaryKey, final Collection<KeySemaphoreImpl> expected) {
     mockery.checking(new Expectations() {
      {
        one(delegate).getKeySemamphoresOfPrimaryIndexKey(primaryKey);
        will(returnValue(expected));
      }
    });
  }

  private void mockPrimaryIndexKeyExistsInDelegate(final Object primaryKey, final boolean exists) {
    mockery.checking(new Expectations() {
      {
        one(delegate).doesPrimaryIndexKeyExist(primaryKey);
        will(returnValue(exists));
      }
    });
  }

  private void mockThatPrimaryKeyExistsInCache(final Object primaryKey, final boolean exists) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(with(same(CacheKeyBuilder.Mode.key)), with(anything()));
        will(returnValue(cacheKey));
        one(cache).exists(cacheKey);
        will(returnValue(exists));
      }
    });
  }

  private void mockThatPrimaryIndexKeyCacheReadThrowsException(final Object primaryKey, final Object cacheKey, final CacheKeyBuilder.Mode mode) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(mode, primaryKey);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(throwException(new RuntimeException()));
      }
    });
  }
  
  private void mockThatCacheKeyExistsForPrimaryIndexKeyThrowsException(final Object primaryKey) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(CacheKeyBuilder.Mode.key, primaryKey);
        will(returnValue(cacheKey));
        one(cache).exists(cacheKey);
        will(throwException(new RuntimeException()));
      }
    });
  }

  private void mockThatCacheWriteThrowsException(final String cacheKey) {
    mockery.checking(new Expectations() {
      {
        one(cache).put(with(same(cacheKey)), with(anything()));
        will(throwException(new RuntimeException()));
      }
    });
  }

  private void mockThatGetNodeIdsOfPrimaryIndexKeyReturnsValue(final Object primaryKey, final Collection<Integer> expected) {
    mockery.checking(new Expectations() {
      {
        one(delegate).getNodeIdsOfPrimaryIndexKey(primaryKey);
        will(returnValue(expected));
      }
    });
  }

  private void mockThatPrimaryIndexKeyIsFoundInCache(final Object primaryKey, final Collection<Integer> expected, final CacheKeyBuilder.Mode mode) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(mode, primaryKey);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(returnValue(expected));
      }
    });
  }

  private void mockThatKeyIsPutIntoCache(final String cacheKey, final Object value) {
    mockery.checking(new Expectations() {
      {
        one(cache).put(cacheKey, value);
      }
    });
  }

  private void mockThatPrimaryIndexKeyIsNotFoundInCache(final Object primaryKey, CacheKeyBuilder.Mode mode) {
    mockThatPrimaryIndexKeyIsFoundInCache(primaryKey, null, mode);
  }
}
