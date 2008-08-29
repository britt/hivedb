package org.hivedb.test;

import org.hibernate.shards.util.Lists;
import org.hivedb.util.functional.Atom;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TestNGTools {

	public static Iterator<Object[]> makeObjectArrayIterator(Collection items) {
		List<Object[]> iterable = Lists.newArrayList();
		Object prototype = Atom.getFirstOrThrow(items);
		for(Object item : items){
			if(prototype.getClass().isInstance(Collection.class))
				iterable.add(((Collection)item).toArray());
			else if(prototype.getClass().isArray())
				iterable.add((Object[]) item);
			else
				iterable.add(new Object[]{item});
		}
		return iterable.iterator();
	}
}
