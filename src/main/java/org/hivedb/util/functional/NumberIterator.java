/**
 * 
 */
package org.hivedb.util.functional;

import java.util.Iterator;

public class NumberIterator implements Iterator<Number>, Iterable<Number> {
	int number;
	int current=0;
	public NumberIterator(int number)
	{
		this.number = number;
	}
	public boolean hasNext() {
		return current < number;
	}

	public Number next() {
		return ++current;
	}

	public void remove() {
	}
	
	public Iterator<Number> iterator() {
		return this;
	}
}