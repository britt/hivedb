package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Collect {
	public static abstract class Function<R> {public abstract R f();}
	
	@SuppressWarnings("unchecked")
	public static<R> Collection<R> create(Function<R> function, Iterator iterator)
	{
		List<R> list = new ArrayList<R>();
		for (;iterator.hasNext();iterator.next())
			list.add(function.f());    				
		return list;
	}
	
	public static<I,R> Collection<R> amass(Unary<I,R> function, Iterable<I> iterable) {
		List<R> list = new ArrayList<R>();
		Iterator<I> iterator = iterable.iterator();
		while(iterator.hasNext())
			list.add(function.f(iterator.next()));    				
		return list;
	}
	
	public static<T> Pair<Collection<T>,Collection<T>> diff(Collection<T> left, Collection<T> right) {
		Collection<T> leftOnly = new ArrayList<T>();
		Collection<T> rightOnly = new ArrayList<T>();
		
		for(T item : left)
			if(!right.contains(item))
				leftOnly.add(item);
		
		for(T item : right)
			if(!left.contains(item))
				rightOnly.add(item);
		
		return new Pair<Collection<T>, Collection<T>>(leftOnly,rightOnly);
	}
}
