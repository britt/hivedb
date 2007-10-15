package org.hivedb.util.proxy;

public abstract class BenchmarkingProxy<R> {
	private long start, stop;
	
	@SuppressWarnings("unchecked")
	public R execute(){
		R output;
		
		start = System.nanoTime();
		try{
			output = doWork();
			stop = System.nanoTime();
			onSuccess(output);
		} catch( Throwable e) {
			onFailure();
			stop = System.nanoTime();
			if(RuntimeException.class.isInstance(e))
				throw (RuntimeException)e;
			else
				throw new RuntimeException(e);
		} finally {
			onFinally();
		}
		return output;
	}
	
	public long getRuntimeInNanos() {
		return stop - start;
	}
	
	protected long getStartTime() { return start; }
	protected long getStopTime() { return stop; }
	
	public long getRuntimeInMillis() {
		return getRuntimeInNanos() / 1000000;
	}
	
	protected abstract R doWork();
	protected abstract void onSuccess(R output);
	protected abstract void onFailure();
	protected void onFinally() {}
}
