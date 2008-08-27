package org.hivedb.util.functional;

import java.util.*;

public class Atom {
	public static<T> T getFirst(Iterable<? extends T> iterable) throws Exception
	{
		for (T item : iterable)
			return item;
		throw new Exception("Iterable has no items");
	}
	public static<T> T getFirstOrThrow(Iterable<? extends T> iterable)
	{
		for (T item : iterable)
			return item;
		throw new NoSuchElementException("Iterable has no items");
	}
	public static<T> T getFirstOrNull(Iterable<? extends T> iterable)
	{
		for (T item : iterable)
			return item;
		return null;
	}
	public static Object getFirstOrDefault(Iterable<?> iterable, Object defaultItem)
	{
		for (Object item : iterable)
			return item;
		return defaultItem;
	}
	public static Class<?> getClassFirstOrDefault(Iterable<?> iterable, Class<?> defaultClass)
	{
		for (Object item : iterable)
			return item.getClass();
		return defaultClass;
	}
	
	public static<T> Collection<T> getRest(Iterable<? extends T> iterable) throws Exception
	{
		List<T> results = new ArrayList<T>();
		for (T item: iterable)
			results.add(item);
		if (results.size() == 0)
			throw new Exception("Iterable has no items");
		results.remove(0);
		return results;
	}
	public static<T> Collection<T> getRestOrThrow(Iterable<? extends T> iterable) {
		try {
			return getRest(iterable);
		}
		catch (Exception e) { throw new NoSuchElementException(e.getMessage()); }
	}
	
	public static<T> T getFirst(T[] array) throws Exception
	{
		if(array.length == 0)
			throw new Exception("Array has no items");
		else
			return array[0];
	}
	public static<T> T getFirstOrThrow(T[] array)
	{
		if(array.length == 0)
			throw new NoSuchElementException("Array has no items");
		else
			return array[0];
	}
	public static<T> T getFirstOrNull(T[] array)
	{
		if(array.length == 0)
			return null;
		else
			return array[0];
	}
	
	public static<T> T getLast(Iterable<? extends T> iterable) throws Exception
	{
		Iterator<? extends T> i = iterable.iterator();
		T item = null;
		while( i.hasNext())
			item = i.next();
		
		if( item != null)
			return item;
		else
			throw new Exception("Iterable has no items");
	}
	public static<T> T getLastOrThrow(Iterable<? extends T> iterable)
	{
		Iterator<? extends T> i = iterable.iterator();
		T item = null;
		while( i.hasNext())
			item = i.next();
		
		if( item != null)
			return item;
		else
			throw new NoSuchElementException("Iterable has no items");
	}
	public static<T> T getLastOrNull(Iterable<? extends T> iterable)
	{
		Iterator<? extends T> i = iterable.iterator();
		T item = null;
		while( i.hasNext())
			item = i.next();
		
		return item;
	}
	public static<T> T getLast(T[] array) throws Exception
	{
		if(array.length > 0)
			return array[array.length-1];
		else
			throw new Exception("Array has no items");
	}
	public static<T> T getLastOrThrow(T[] array)
	{
		if(array.length > 0)
			return array[array.length-1];
		else
			throw new NoSuchElementException("Array has no items");
	}
	public static<T> T getLastOrNull(T[] array)
	{
		if(array.length > 0)
			return array[array.length-1];
		else
			return null;
	}
}
