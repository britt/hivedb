/**
 * 
 */
package org.hivedb.util.functional;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.hivedb.util.ReflectionTools;

public class DebugMap<K,V> implements Map<K,V>
{
	protected Map<K,V> map;
	protected boolean showHashes;
	public DebugMap()
	{
		map = new Hashtable<K,V>();
	}
	public DebugMap(Map<K,V> map)
	{
		this.map = map;
	}
	public DebugMap(Map<K,V> map, boolean showHashes)
	{
		this.map = map;
		this.showHashes = showHashes;
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
	
	public String toString() {
		return 
		(String)Amass.join(
			new Joiner.ConcatStrings<String>("\n"),
			Transform.map(new Unary<Entry<K,V>, String>() {
					public String f(Entry entry) {
						return entry.getKey().toString() + " -> " + entry.getValue().toString() + 
						(showHashes ? "(Hash: " + makeHashCode(entry) + ")" : "");
				}},
				map.entrySet()),
			"");
	}
	private int makeHashCode(Entry entry) {
		if (entry.getValue() == null)
			return 0;
		return ReflectionTools.doesImplementOrExtend(entry.getValue().getClass(), Collection.class) 
			? Amass.makeHashCode((Collection)entry.getValue()) 
			: entry.getValue().hashCode();
	}
}