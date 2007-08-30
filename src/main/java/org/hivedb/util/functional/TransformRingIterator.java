/**
 * 
 */
package org.hivedb.util.functional;

import java.util.Collection;
import java.util.Iterator;

class TransformRingIterator<INPUT, OUTPUT> implements Iterator<OUTPUT>
{
	Collection<INPUT> collection;
	Iterator<INPUT> iterator; 
	Unary<INPUT,OUTPUT> transformFunction;
	public TransformRingIterator(Unary<INPUT,OUTPUT> transformFunction, Collection<INPUT> collection)
	{
		this.collection = collection;
		this.iterator = collection.iterator();
		this.transformFunction = transformFunction;
	}
	public OUTPUT next() {
		if (!iterator.hasNext())
			iterator = collection.iterator();
		return transformFunction.f(iterator.next());			
	}
	public boolean hasNext() {
		return true;
	}
	public void remove() {}
}