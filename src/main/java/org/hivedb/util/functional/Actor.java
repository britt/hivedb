package org.hivedb.util.functional;

import java.util.Collection;
import java.util.Collections;

/**
 *  A simple class for performing an operation on either a scalar value or collection of values
 *  The given operation is performed on the scalar or on each iteration of the collection
 * @author Andy
 *
 * @param <T> The type of the scalar or of each type of the collection
 */
public abstract class Actor<T>{
	private Object obj;
	public Actor(Object obj) {
		this.obj = obj;
	}
	public abstract void f(T t);
	@SuppressWarnings("unchecked")
	public void perform()
	{
		if (obj instanceof Collection)
			for (T t : (Collection<T>)obj)
				f(t);
		else
			f((T)obj);
	}
	
	@SuppressWarnings("unchecked")
	public static Collection<Object> forceCollection(Object obj) {
		if (obj == null)
			return Collections.emptyList();
		if (obj instanceof Collection)
			return (Collection<Object>)obj;
		return Collections.singletonList(obj);
	}

}
