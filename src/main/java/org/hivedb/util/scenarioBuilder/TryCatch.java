package org.hivedb.util.scenarioBuilder;

public abstract class TryCatch<T>
{
	abstract T t() throws Exception;
	abstract T c(Exception e);
	public T go() {
		try { return t(); }
		catch (Exception e) { return c(e); }
	}
}
	
