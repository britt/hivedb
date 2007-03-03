package org.hivedb.util.scenarioBuilder;

public interface Unary<I,R> {
	public R f(I item);
}