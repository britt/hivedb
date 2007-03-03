/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Iterator;

public class NumberIterator implements Iterator {
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