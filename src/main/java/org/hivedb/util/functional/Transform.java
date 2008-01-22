package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// Class to map each item in a set to another value
public class Transform {
	public static<I,R> Collection<R> map(Unary<I,R> mapper, Iterable<? extends I> iterable)
	{
		List<R> list = new ArrayList<R>();
		for (I item : iterable)
			list.add(mapper.f(item));    				
		return list;
	}
	
	public static <OUTER, INNER> Collection<INNER> flatMap(Unary<OUTER, Collection<INNER>> mapper, Iterable<? extends OUTER> iterable)
	{
		List<INNER> results = new ArrayList<INNER>();
		for (Collection<INNER> collection : map(mapper, iterable))
			results.addAll(collection);
		return results;
	}
	
	public static<T> Collection<T> toCollection(T scalar)
	{
		List<T> collection = new ArrayList<T>();
		collection.add(scalar);
		return collection;
	}
	public static<T> Collection<T> toCollection(Iterable<T> iterable)
	{
		List<T> collection = new ArrayList<T>();
		for (T t : iterable)
			collection.add(t);
		return collection;
	}
	public static<K,V,I> Map<K,V> toMap(Unary<I,K> keyMapper, Unary<I,V> valueMapper, Iterable<I> iterable)
	{
		Map<K,V> map = new DebugMap<K,V>();
		for (I item : iterable)
			map.put(keyMapper.f(item),valueMapper.f(item));
		return map;
	}
	public static<K,V,I> Map<K,V> toMap(Unary<I,K> keyMapper, Unary<I,V> valueMapper, Iterable<I> iterable, Predicate<Entry<K,V>> onNull)
	{
		Map<K,V> map = new DebugMap<K,V>();
		for (I item : iterable) {
			final K key = keyMapper.f(item);
			final V value = valueMapper.f(item);
			if ((key == null || value == null) && !onNull.f(new Pair<K,V>(key,value)))
				continue;
			map.put(key,value);		
		}
		return map;
	}
	public static<K,V> Map<K,V> toMap(Entry<K,V>[] entries)
	{
		return toMap(Arrays.asList(entries));
	}
	
	public static<K,V> Map<K,V> toMap(Collection<? extends Entry<K,V>> entries)
	{
		Map<K,V> map = new DebugMap<K,V>();
		for (Entry<K,V> entry : entries)
			map.put(entry.getKey(), entry.getValue());
		return map;
	}

	public static<K,V,I> Map<K,Collection<V>> toMapCollection(Unary<I,K> keyMapper, Unary<I,V> valueMapper, Iterable<I> iterable)
	{
		Map<K,Collection<V>> map = new DebugMap<K,Collection<V>>();
		for (I item : iterable) {
			K keyMapValue = keyMapper.f(item);
			if (!map.containsKey(keyMapValue)) 
				map.put(keyMapValue, new ArrayList<V>());
			map.get(keyMapValue).add(valueMapper.f(item));	
		}
		return map;
	}
	public static <K1,V1K2, V2> Map<K1,V2> connectMaps(Map<K1, V1K2> inputMap, Map<V1K2,V2> outputMap)
	{
		Map<K1,V2> finalMap = new DebugMap<K1,V2>();
		for (Entry<K1,V1K2> inputEntry : inputMap.entrySet())
			if (outputMap.containsKey(inputEntry.getValue()))
				finalMap.put(inputEntry.getKey(), outputMap.get(inputEntry.getValue()));
		return finalMap;
	}

	public static class IdentityFunction<I> implements Unary<I, I>
	{
		public I f(I item) {
			return item;
		}
	}
	public static class MapToKeyFunction<K,V> implements Unary<Entry<K,V>, K>
	{
		public K f(Entry<K,V> item) {
			return item.getKey();
		}
	}
	public static class MapToValueFunction<K,V> implements Unary<Entry<K,V>, V>
	{
		public V f(Entry<K,V> item) {
			return item.getValue();
		}
	}
	public static<T> Collection<T> flatten(Collection<Collection<T>> collection) {
		Collection<T> results = new ArrayList<T>();
		for (Collection<T> c : collection)
			results.addAll(c);
		return results;
	}
	public static<T> Collection<T> flatten(Collection<T>... collections) {
		Collection<T> results = new ArrayList<T>();
		for (Collection<T> c : collections)
			results.addAll(c);
		return results;
	}
	public static<K,V> Map<K,V> toOrderedMap(Entry<K,V>[] entries) {
		Map<K,V> map = new OrderedDebugMap<K,V>();
		for (Entry<K,V> entry : entries)
			map.put(entry.getKey(), entry.getValue());
		return map;
	}
	public static<K,V> Map<K,V> toOrderedMap(Collection<Entry<K,V>> entries)
	{
		Map<K,V> map = new OrderedDebugMap<K,V>();
		for (Entry<K,V> entry : entries)
			map.put(entry.getKey(), entry.getValue());
		return map;
	}
	public static<K,V,I> Map<K,V> toOrderedMap(Unary<I,K> keyMapper, Unary<I,V> valueMapper, Iterable<? extends I> iterable)
	{
		Map<K,V> map = new OrderedDebugMap<K,V>();
		for (I item : iterable)
			map.put(keyMapper.f(item), valueMapper.f(item));
		return map;
	}

	public static<K,V> Map<V, K> reverseMap(Map<K, V> map) {
		return
			Transform.toMap(new MapToValueFunction<K,V>(),
							new MapToKeyFunction<K,V>(),
							map.entrySet());
	}

	@SuppressWarnings("unchecked")
	public static<F,T> Collection<T> castCollection(Collection<F> collection)
	{
		return (Collection<T>)collection;
	}
	
	public static int[] toIntArray(Collection<Integer> collection)
	{
		int[] stupid = new int[collection.size()];
		int i = 0;
		for (int item : collection)
			stupid[i++] = item;
		return stupid;
	}
	public static class MapKeyToValueFunction<K, V> implements Unary<K, V>
	{
		Map<K,V> map;
		public MapKeyToValueFunction(Map<K,V> map) {
			this.map = map;
		}
		public V f(K key) {
			return map.get(key);
		}
	}
}

