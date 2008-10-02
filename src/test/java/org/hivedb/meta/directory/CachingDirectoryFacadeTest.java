package org.hivedb.meta.directory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.Hive;
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

  private void mockPrimaryIndexKeyExistsInDelegate(final Object primaryKey, final boolean exists) {
    mockery.checking(new Expectations() {
      {
        one(delegate).doesPrimaryIndexKeyExist(primaryKey);
        will(returnValue(exists));
      }
    });
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

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldDelegateIfCacheThrowsExceptionOnRead() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatCacheReadThrowsException(primaryKey, cacheKey);
    mockThatDelegateReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  private void mockThatCacheReadThrowsException(final Object primaryKey, final Object cacheKey) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(CacheKeyBuilder.Mode.key, primaryKey);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(throwException(new RuntimeException()));
      }
    });
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldStillReturnIfCacheWriteFails() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatKeyIsNotFoundInCache(primaryKey);
    mockThatDelegateReturnsValue(primaryKey, expected);
    mockThatCacheWriteThrowsException(cacheKey);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  private void mockThatCacheWriteThrowsException(final String cacheKey) {
    mockery.checking(new Expectations() {
      {
        one(cache).put(with(same(cacheKey)), with(anything()));
        will(throwException(new RuntimeException()));
      }
    });
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldNotDelegateIfFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();
    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);

    mockThatKeyIsFoundInCache(primaryKey, expected);

    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from cache", expected, actual);
  }

  @Test
  public void getNodeIdsOfPrimaryIndexKeyshouldDelegateIfNotFoundInCache() throws Exception {
    final Object primaryKey = new Object();
    final Collection<Integer> expected = Lists.newArrayList();

    mockThatKeyIsNotFoundInCache(primaryKey);
    mockThatDelegateReturnsValue(primaryKey, expected);
    mockThatKeyIsPutIntoCache(cacheKey, expected);

    CachingDirectoryFacade facade = new CachingDirectoryFacade(delegate, cache, keyBuilder);
    Collection<Integer> actual = facade.getNodeIdsOfPrimaryIndexKey(primaryKey);
    assertSame("return value not from delegate", expected, actual);
  }

  private void mockThatDelegateReturnsValue(final Object primaryKey, final Collection<Integer> expected) {
    mockery.checking(new Expectations() {
      {
        one(delegate).getNodeIdsOfPrimaryIndexKey(primaryKey);
        will(returnValue(expected));
      }
    });
  }

  private void mockThatKeyIsFoundInCache(final Object primaryKey, final Collection<Integer> expected) {
    mockery.checking(new Expectations() {
      {
        one(keyBuilder).build(CacheKeyBuilder.Mode.key, primaryKey);
        will(returnValue(cacheKey));
        one(cache).get(cacheKey);
        will(returnValue(expected));
      }
    });
  }

  private void mockThatKeyIsPutIntoCache(final String cacheKey, final Collection<Integer> value) {
    mockery.checking(new Expectations() {
      {
        one(cache).put(cacheKey, value);
      }
    });
  }

  private void mockThatKeyIsNotFoundInCache(final Object primaryKey) {
    mockThatKeyIsFoundInCache(primaryKey, null);
  }
}
