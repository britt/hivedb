package org.hivedb.util.functional;

public abstract class TryFail<T> extends TryCatch<T>
{
	T c(Exception e) { throw new RuntimeException(e); }
}