package org.hivedb.util.functional;


public abstract class ExceptionalActor<T,E extends Exception>  {

	private Object obj;
	public ExceptionalActor(Object obj) {
		this.obj = obj;
	}

	public abstract void f(T t) throws E;
}
