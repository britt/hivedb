package org.hivedb.util.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Generate {
	public static<R> Collection<R> create(Generator<R> function, Iterator iterator)
	{
		List<R> list = new ArrayList<R>();
		for (;iterator.hasNext();iterator.next())
			list.add(function.f());    				
		return list;
	}
}
