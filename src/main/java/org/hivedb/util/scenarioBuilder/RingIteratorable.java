/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;
import java.util.Iterator;

public class RingIteratorable<T> implements Iterator<T>, Iterable<T>
{
	Collection<T> collection;
	Iterator<T> iterator; 
	int stopAfterThisManyCalls = -1;
	int numberOfCalls;
	
	public RingIteratorable(Collection<T> collection)
	{
		this.collection = collection;
		this.iterator = collection.iterator();
	}
	public RingIteratorable(Collection<T> collection, int stopAfterThisManyCalls)
	{
		this.collection = collection;
		this.iterator = collection.iterator();
		this.stopAfterThisManyCalls = stopAfterThisManyCalls;
	}
	
	public boolean hasNext() {
		return (stopAfterThisManyCalls < 0 || numberOfCalls < stopAfterThisManyCalls);
	}

	public T next() {
		if (stopAfterThisManyCalls >=0)
			numberOfCalls++;
		
		if (!iterator.hasNext())
			iterator = collection.iterator();
		return iterator.next();			
	}

	public void remove() {}
	public Iterator<T> iterator() {
		return this;
	}
}