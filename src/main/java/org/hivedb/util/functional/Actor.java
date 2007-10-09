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
public abstract class Actor<T> extends ExceptionalActor<T, RuntimeException>  {
	public Actor(Object obj) {
		super(obj);
	}
	@SuppressWarnings("unchecked")
	public static Collection<Object> forceCollection(Object obj) {
		if (obj instanceof Collection)
			return (Collection<Object>)obj;
		else
			return Collections.singletonList(obj);
	}

}
