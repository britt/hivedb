package org.hivedb;

import org.hivedb.management.statistics.Counter;

public abstract class StatisticsProxy<R, E extends Throwable> extends BenchmarkingProxy<R,E> {
	protected String failureKey, successKey, timeKey;
	protected Counter counter;
	
	public StatisticsProxy(Counter counter, String successKey, String failureKey, String timeKey) {
		this.counter = counter;
		this.successKey = successKey;
		this.failureKey = failureKey;
		this.timeKey = timeKey;
	}
	
	@Override
	protected abstract R doWork() throws E;

	@Override
	protected void onFailure() {
		counter.increment(failureKey);
	}

	@Override
	protected void onSuccess(R output) {
		counter.increment(successKey);
		counter.add(timeKey, getRuntimeInMillis());
	}
}
