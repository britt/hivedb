/**
 * 
 */
package org.hivedb.util.database.test;

import java.util.Iterator;

public class TestIterator implements Iterator<Object[]> {
	Iterator<LazyInitializer> i;
	SchemaInitializer test;
	public TestIterator(Iterator<LazyInitializer> i, SchemaInitializer test) { 
		this.i = i;
		this.test = test;
	}
	
	public Object[] next() {return new Object[]{
			i.next().setTest(test).f()};
	}
	public void remove() {throw new UnsupportedOperationException();}
	public boolean hasNext() {return i.hasNext();}
	
}