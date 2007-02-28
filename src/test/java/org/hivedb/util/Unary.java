package org.hivedb.util;

public interface Unary<I,R> {
	public R f(I item);
}