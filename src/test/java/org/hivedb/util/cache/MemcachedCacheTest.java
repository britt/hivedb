package org.hivedb.util.cache;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.util.functional.Delay;
import org.hivedb.util.functional.VoidDelay;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Currently tests constructors and the {@link Cache} interface methods.
 */
public class MemcachedCacheTest {
  private final static Log log = LogFactory.getLog(MemcachedCacheTest.class);

  private Mockery mockery;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }


  @Test(expected = IllegalStateException.class)
  public void constructionWithUninitializedDefaultPoolShouldFail() throws Exception {
    new MemcachedCache();
  }

  @Test(expected = IllegalStateException.class)
  public void constructionWithUninitializedNamedPoolShouldFail() throws Exception {
    new MemcachedCache("foo");
  }

  @Test(expected = IllegalStateException.class)
  public void constructionWithUninitializedExplicitPoolShouldFail() throws Exception {
    new MemcachedCache(null, SockIOPool.getInstance());
  }

  @Test
  public void constructionWithInitializedDefaultPoolShouldNotThrowException() throws Exception {
    initializeDefaultPool();
    new MemcachedCache();
  }

  private void initializeDefaultPool() {
    SockIOPool pool = SockIOPool.getInstance();
    assertFalse("something else already initialized this pool", pool.isInitialized());
    pool.setServers(new String[]{"127.0.0.1:11211"});
    pool.initialize();
  }

  private String initializeUniquePool() {
    String name = this.getClass().getName() + System.nanoTime();
    SockIOPool pool = SockIOPool.getInstance(name);
    assertFalse(pool.isInitialized());
    pool.setServers(new String[]{"127.0.0.1:11211"});
    pool.initialize();
    return name;
  }

  @Test
  public void constructionWithUninitializedNamedPoolShouldNotThrowException() throws Exception {
    String poolName = initializeUniquePool();
    new MemcachedCache(poolName);
  }

  @Test
  public void constructionWithUninitializedExplicitPoolShouldNotThrowException() throws Exception {
    String poolName = initializeUniquePool();
    new MemcachedCache(null, SockIOPool.getInstance(poolName));
  }

  @Test
  public void shouldDelegateToClientOnGet() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).get("foo");
        will(returnValue("bar"));
      }
    });
    assertEquals("bar", cache.get("foo"));
  }

  @Test
  public void shouldThrowDelegateExceptionOnGet() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    final Exception expected = new RuntimeException();
    final MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).get("foo");
        will(throwException(expected));
      }
    });
    assertExceptionThrown(expected, new Delay() {
      public Object f() {
        return cache.get("foo");
      }
    });
  }

  @Test
  public void shouldDelegateToClientOnPut() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).set("foo", "bar");
      }
    });
    cache.put("foo", "bar");
  }

  @Test
  public void shouldThrowDelegateExceptionOnPut() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    final Exception expected = new RuntimeException();
    final MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).set("foo", "bar");
        will(throwException(expected));
      }
    });
    assertExceptionThrown(expected, new VoidDelay() {
      public void f() {
        cache.put("foo", "bar");
      }
    });
  }

  @Test
  public void shouldDelegateToClientOnExists() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        exactly(2).of(client).keyExists("foo");
        will(onConsecutiveCalls(returnValue(true), returnValue(false)));
      }
    });
    //consecutive calls to guard against initialization bug (defaulting to true or false)
    assertTrue(cache.exists("foo"));
    assertFalse(cache.exists("foo"));
  }

  @Test
  public void shouldThrowDelegateExceptionOnExists() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    final Exception expected = new RuntimeException();
    final MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).keyExists("foo");
        will(throwException(expected));
      }
    });
    assertExceptionThrown(expected, new Delay() {
      public Object f() {
        return cache.exists("foo");
      }
    });
  }

  private void assertExceptionThrown(Exception expected, Delay call) {
    try {
      call.f();
      fail("exception expected");
    } catch (RuntimeException e) {
      assertSame(expected, e);
    }
  }

  private void assertExceptionThrown(Exception expected, VoidDelay call) {
    try {
      call.f();
      fail("exception expected");
    } catch (RuntimeException e) {
      assertSame(expected, e);
    }
  }

  @Test
  public void shouldDelegateToClientOnDelete() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).delete("foo");
      }
    });
    cache.remove("foo");
  }

  @Test
  public void shouldThrowDelegateExceptionOnDelete() throws Exception {
    String poolName = initializeUniquePool();
    final MemCachedClient client = mockery.mock(MemCachedClient.class, "client");
    final Exception expected = new RuntimeException();
    final MemcachedCache cache = new MemcachedCache(client, SockIOPool.getInstance(poolName));
    mockery.checking(new Expectations() {
      {
        one(client).delete("foo");
        will(throwException(expected));
      }
    });
    assertExceptionThrown(expected, new VoidDelay() {
      public void f() {
        cache.remove("foo");
      }
    });
  }

}
