package org.hivedb.util.functional;


public abstract class Runtimer<T> implements DelayedTry<T> {

	public T go() {
		try {
			return f();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
