/**
 * 
 */
package org.hivedb.util.functional;

import java.util.Iterator;

public class NumberIterator implements Iterator<Integer>, Iterable<Integer> {
	int number;
	int current=0;
	public NumberIterator(int start, int end)
	{
		this.current = start;
		this.number = end;
	}

	public NumberIterator(int number)
	{
		this.number = number;
	}
	public boolean hasNext() {
		return current < number;
	}

	public Integer next() {
		return ++current;
	}

	public void remove() {
	}
	
	public Iterator<Integer> iterator() {
		return this;
	}
}