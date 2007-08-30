package org.hivedb;

import org.hivedb.management.statistics.Counter;

public abstract class StatisticsProxy<R> extends BenchmarkingProxy<R> {
	private static final String count = "Count";
	private static final String failures = "Failures";
	private static final String time = "Time";
	
	protected String failureKey, successKey, timeKey;
	protected Counter counter;
	
	public StatisticsProxy(Counter counter, String baseKey) {
		this(counter, baseKey+count, baseKey+failures, baseKey+time);
	}
	
	public StatisticsProxy(Counter counter, String successKey, String failureKey, String timeKey) {
		this.counter = counter;
		this.successKey = successKey;
		this.failureKey = failureKey;
		this.timeKey = timeKey;
	}
	
	@Override
	protected abstract R doWork();

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
