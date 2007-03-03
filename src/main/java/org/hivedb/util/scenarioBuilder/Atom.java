package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Atom {
	public static<T> T getFirst(Iterable<T> iterable) throws Exception
	{
		for (T item : iterable)
			return item;
		throw new Exception("Iterable has no items");
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
}
