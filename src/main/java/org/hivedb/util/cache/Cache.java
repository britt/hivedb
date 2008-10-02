package org.hivedb.util.cache;

/**
 * Simple cache abstraction.
 *
 * @author Britt Crawford <bcrawford@cafepress.com>
 * @author Dave Peckham <dpeckham@cafepress.com>
 */
public interface Cache<K, V> {
  V get(K key);

  void put(K key, V value);

  boolean exists(K key);

  void remove(K key);
}
