package org.hivedb.util.functional;

public interface Unary<I,R> {
	public R f(I item);
}