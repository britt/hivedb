package org.hivedb.util.functional;


public abstract class DelayForArgument<A, T> {

	public abstract T f(A a);

	private T delayed;
	public T advance(A a) {
		delayed = f(a);
		return delayed;
	}
	public T get()
	{
		return delayed;
	}
}
