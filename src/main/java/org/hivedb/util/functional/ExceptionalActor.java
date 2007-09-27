package org.hivedb.util.functional;

import java.util.Collection;

public abstract class ExceptionalActor<T,E extends Exception>  {

	private Object obj;
	public ExceptionalActor(Object obj) {
		this.obj = obj;
	}

	public abstract void f(T t) throws E;
	@SuppressWarnings("unchecked")
	public void perform() throws E
	{
		if (obj instanceof Collection)
			for (T t : (Collection<T>)obj)
				f(t);
		else
			f((T)obj);
	}
}
