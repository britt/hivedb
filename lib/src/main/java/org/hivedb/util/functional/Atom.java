package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Atom {
	public static<T> T getFirst(Iterable<T> iterable) throws Exception
	{
		for (T item : iterable)
			return item;
		throw new Exception("Iterable has no items");
	}
	public static<T> T getFirstOrThrow(Iterable<T> iterable)
	{
		for (T item : iterable)
			return item;
		throw new RuntimeException("Iterable has no items");
	}
	public static<T> T getFirstOrNull(Iterable<T> iterable)
	{
		for (T item : iterable)
			return item;
		return null;
	}
	
	public static<T> Collection<T> getRest(Iterable<T> iterable) throws Exception
	{
		List<T> results = new ArrayList<T>();
		for (T item: iterable)
			results.add(item);
		if (results.size() == 0)
			throw new Exception("Iterable has no items");
		results.remove(0);
		return results;
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
			throw new RuntimeException("Array has no items");
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
	
	public static<T> T getLast(Iterable<T> iterable) throws Exception
	{
		Iterator<T> i = iterable.iterator();
		T item = null;
		while( i.hasNext())
			item = i.next();
		
		if( item != null)
			return item;
		else
			throw new Exception("Iterable has no items");
	}
	public static<T> T getLastOrThrow(Iterable<T> iterable)
	{
		Iterator<T> i = iterable.iterator();
		T item = null;
		while( i.hasNext())
			item = i.next();
		
		if( item != null)
			return item;
		else
			throw new RuntimeException("Iterable has no items");
	}
	public static<T> T getLastOrNull(Iterable<T> iterable)
	{
		Iterator<T> i = iterable.iterator();
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
			throw new RuntimeException("Array has no items");
	}
	public static<T> T getLastOrNull(T[] array)
	{
		if(array.length > 0)
			return array[array.length-1];
		else
			return null;
	}
}
