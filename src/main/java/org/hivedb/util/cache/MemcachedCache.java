package org.hivedb.util.cache;

import com.danga.MemCached.ErrorHandler;
import com.danga.MemCached.MemCachedClient;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Very "dumb" {@link org.hivedb.util.cache.Cache} implementation backed by memcached.
 * <p/>
 * It is entierly this cache client's responsibility to deal with any exceptions thrown by memcached.
 * <p/>
 * Cache entries have no expiration. It is also the client's responsibility to properly configure the
 * {@link com.danga.MemCached.SockIOPool}. See memcached javadocs for more information.
 *
 * @author Dave Peckham <dpeckham@cafepress.com>
 */
public class MemcachedCache implements Cache<String, Object> {
  protected static Logger log = Logger.getLogger(MemcachedCache.class);

  private MemCachedClient client;

  public MemcachedCache() {
    this.client = new MemCachedClient();
  }

  public MemcachedCache(String poolName) {
    this.client = new MemCachedClient(poolName);
  }

  public Object get(String key) {
    return client.get(key);
  }

  public void put(String key, Object value) {
    client.set(key, value);
  }

  public boolean exists(String cacheKey) {
    return client.keyExists(cacheKey);
  }

  public void remove(String key) {
    client.delete(key);
  }

  public void setCompressEnable(boolean enable) {
    client.setCompressEnable(enable);
  }

  public void setErrorHandler(ErrorHandler errorHandler) {
    client.setErrorHandler(errorHandler);
  }

  public void setDefaultEncoding(String encoding) {
    client.setDefaultEncoding(encoding);
  }

  public void setPrimitiveAsString(boolean primitiveAsString) {
    client.setPrimitiveAsString(primitiveAsString);
  }

  public void setSanitizeKeys(boolean santitizeKeys) {
    client.setSanitizeKeys(santitizeKeys);
  }

  public void setCompressThreshold(long threshold) {
    client.setCompressThreshold(threshold);
  }

  public boolean flushAll() {
    return client.flushAll();
  }

  public Map stats() {
    return client.stats();
  }
}
