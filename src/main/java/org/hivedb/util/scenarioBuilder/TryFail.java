package org.hivedb.util.scenarioBuilder;

public abstract class TryFail<T> extends TryCatch<T>
{
	T c(Exception e) { throw new RuntimeException(e); }
}