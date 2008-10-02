package org.hivedb.util.cache;

public interface Cache<K, V> {
  V get(K key);

  void put(K key, V value);

  boolean exists(K cacheKey);

  void remove(K key);
}
