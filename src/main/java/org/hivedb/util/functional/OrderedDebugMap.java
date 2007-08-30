package org.hivedb.util.functional;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

class OrderedDebugMap<K,V> extends DebugMap<K,V>
{
	public OrderedDebugMap()
	{
		map = new LinkedHashMap<K,V>();
	}
	
	
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
		return new LinkedHashSet<Entry<K,V>>(
		Transform.map(new Unary<K, Map.Entry<K,V>>() {
			public Map.Entry<K,V> f(K item) {
				return new Pair<K,V>(item, map.get(item));
			}}, map.keySet()));
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
	
	public String toString() {
		return 
		(String)Amass.join(
			new Joiner.ConcatStrings<String>("\n"),
			Transform.map(new Unary<Entry<K,V>, String>() {
					public String f(Entry<K,V> entry) {
						return entry.getKey().toString() + " -> " + entry.getValue().toString();
				}},
				map.entrySet()));
	}
}