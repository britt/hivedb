package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Collect {
	public static abstract class Function<R> {public abstract R f();}
	
	public static<R> Collection<R> create(Function<R> function, Iterator iterator)
	{
		List<R> list = new ArrayList<R>();
		for (;iterator.hasNext();iterator.next())
			list.add(function.f());    				
		return list;
	}
	
	public static<I,R> Collection<R> amass(Unary<I,R> function, Iterator<I> iterator) {
		List<R> list = new ArrayList<R>();
		while(iterator.hasNext())
			list.add(function.f(iterator.next()));    				
		return list;
	}
	
	public static class NumberIterator implements Iterator {
		int number;
		int current=0;
		public NumberIterator(int number)
		{
			this.number = number;
		}
		public boolean hasNext() {
			return current < number;
		}

		public Object next() {
			return ++current;
		}

		public void remove() {
		}
	}
}
