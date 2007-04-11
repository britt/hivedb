package org.hivedb.util.functional;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;


class DebugMap<K,V> implements Map<K,V>
{
	Map<K,V> map = new Hashtable<K,V>();
	public void clear() {
		map.clear();
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	public V get(Object key) {
		if (!map.containsKey(key))
			throw new RuntimeException(
					String.format("Map does not contain the key %s.\nHere are the map contents:%s",
								  key.toString(),
								  this.toString()));
		return map.get(key);
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public Set<K> keySet() {
		return map.keySet();
	}

	public V put(K key, V value) {
		return map.put(key, value);
	}

	public void putAll(Map<? extends K, ? extends V> t) {
		map.putAll(t);
	}

	public V remove(Object key) {
		return map.remove(key);
	}

	public int size() {
		return map.size();
	}

	public Collection<V> values() {
		return map.values();
	}
	// I can't get the generics to comply in the inner Transform.map
	
	public String toString() {
		return 
		(String)Amass.join(
			new Joiner.ConcatStrings<String>("\n"),
			Transform.map(new Unary<Entry<K,V>, String>() {
					public String f(Entry entry) {
						return entry.getKey().toString() + " -> " + entry.getValue().toString();
				}},
				map.entrySet()),
			"");
	}
}