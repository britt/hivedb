package org.hivedb.util;

import java.util.Collection;

import org.hivedb.StatisticsProxy;
import org.hivedb.management.statistics.Counter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;

/***
 * Common benchmarking proxies used through HiveDB
 * @author bcrawford
 *
 */
public class Proxies {
	/***
	 * Build a statistics proxy to execute an update and record the results
	 * @param successKey Counter to tick on a successful update
	 * @param failureKey Counter to tick on failure
	 * @param timeKey Counter to increment 
	 * @param failureMessage
	 * @param parameters
	 * @param stmtFactory
	 * @param j
	 * @return
	 */
	public static StatisticsProxy<Integer> newJdbcUpdateProxy(
			Counter performanceStatistics,
			String baseKey,
			final Object[] parameters, 
			final PreparedStatementCreatorFactory stmtFactory, 
			final JdbcTemplate j) {
		return new StatisticsProxy<Integer>(performanceStatistics, baseKey) {
			@Override
			protected Integer doWork() {
				return j.update(stmtFactory.newPreparedStatementCreator(parameters));
			}
			@Override
			protected void onSuccess(Integer output) {
				counter.add(successKey,output);
				counter.add(timeKey, getRuntimeInMillis());
			}
		};
	}
	
	public static<T> StatisticsProxy<Collection<T>> newJdbcSqlQueryProxy(
			Counter performanceStatistics,
			String baseKey,
			final String sql,
			final Object[] parameters,
			final RowMapper rowMapper,
			final JdbcTemplate j){
		return new StatisticsProxy<Collection<T>>(performanceStatistics, baseKey) {
			@SuppressWarnings("unchecked")
			@Override
			protected Collection<T> doWork() {
				return j.query(sql,	parameters, rowMapper);
			}
			@Override
			protected void onSuccess(Collection<T> output) {
				counter.add(successKey,output.size());
				counter.add(timeKey, getRuntimeInMillis());
			}
		};
	}
}
