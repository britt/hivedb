package org.hivedb;

import java.util.Collection;

import org.hivedb.management.statistics.Counter;

public  abstract class CollectionProxy<T> extends StatisticsProxy<Collection<T>>{
	public CollectionProxy(Counter counter, String baseKey) {
		super(counter, baseKey);
	}
	@Override
	protected void onSuccess(Collection<T> output) {
		counter.add(successKey, output.size());
		counter.add(timeKey, getRuntimeInMillis());
	}
}