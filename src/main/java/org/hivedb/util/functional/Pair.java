package org.hivedb.util.functional;

import java.util.Map.Entry;

public class Pair<K,V> implements Entry<K,V> {

	private K key;
	private V value;
	public Pair(K key, V value)
	{
		this.key = key;
		this.value = value;
	}
	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public V setValue(V value) {
		this.value = value;
		return value;
	}
	
	public String toString() {
		return String.format("%s : %s", getKey(), getValue());
	}
}
