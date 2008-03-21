/**
 * 
 */
package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PairIterator<T> implements Iterator<Map.Entry<T,T>>, Iterable<Map.Entry<T,T>> {
	private int size;
	private List<T> pairs;
	private int current=0;

	public PairIterator(Collection<T> pairs)
	{
		this.pairs = new ArrayList<T>(pairs);
		if (pairs.size() % 2 != 0)
			throw new RuntimeException(String.format("Uneven number of items in collection: %s", pairs));
		this.size = pairs.size();
	}

	public boolean hasNext() {
		return current < size;
	}

	public Map.Entry<T,T> next() {
		current+=2;
		return new Pair<T,T>(pairs.get(current-2), pairs.get(current-1));
	}

	public void remove() {
	}
	
	public Iterator<Map.Entry<T,T>> iterator() {
		return this;
	}
}