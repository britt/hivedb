package org.hivedb.util.scenarioBuilder;

public interface DelayedTry<T> {
	T f() throws Exception;
}
