package org.hivedb.management.quartz;

import java.util.Calendar;
import java.util.Collection;

import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.StatefulJob;
import org.quartz.TriggerUtils;

/**
 * A naieve nodebalancing implementation.  It will get smarter as 
 * the code evolves but right now its "balance as fast as you can".
 * @author britt
 *
 */
public class NodeBalancingJob implements StatefulJob {
	public static final char JOB_NAME_SEPARATOR = '.';
	public static final String NODE_LIST_KEY = "Nodes";

	// TODO populate these guys
	private NodeBalancer balancer;
	private MigrationEstimator estimator;
	private PartitionKeyStatisticsDao statsDao;
	private PartitionDimension dimension;
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getMergedJobDataMap();
		Collection<Node> nodes = (Collection<Node>) map.get(NODE_LIST_KEY);
		enqueueMoves(balancer.suggestMoves(nodes), context.getScheduler());
	}
	
	private void enqueueMoves(Collection<Migration> migrations, Scheduler scheduler) throws JobExecutionException {
		long delay = 0;
		for(Migration migration : migrations){
			PartitionKeyStatistics stats = statsDao.findByPrimaryPartitionKey(dimension, migration.getMigrantId());
			long executionTime = estimator.estimateMoveTime(stats);
			// TODO Multi-threaded delay computation
			delay += executionTime;
			try {
				scheduler.scheduleJob(
						MigrationJob.createDetail(getJobName(delay, MigrationJob.class), getGroupName(), migration), 
						TriggerUtils.makeImmediateTrigger("Trigger"+delay, 0, delay));
			} catch (SchedulerException e) {
				throw new JobExecutionException(e);
			}
		}
	}
	
	private String getJobName(long delay, Class jobType) {
		StringBuilder s = new StringBuilder(jobType.toString());
		s.append(JOB_NAME_SEPARATOR);
		s.append(Calendar.getInstance().getTime().getTime() + delay);
		return s.toString();
	}
	
	private String getGroupName() {
		StringBuilder s = new StringBuilder("Balancing");
		s.append(JOB_NAME_SEPARATOR);
		s.append(Calendar.getInstance().get(Calendar.MONTH));
		s.append(JOB_NAME_SEPARATOR);
		s.append(Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
		s.append(JOB_NAME_SEPARATOR);
		s.append(Calendar.getInstance().get(Calendar.HOUR_OF_DAY));
		return s.toString();
	}
	
	private static JobDataMap buildJobDataMap(JobDataMap map, Collection<Node> nodeList) {
		map.put(NODE_LIST_KEY, nodeList);
		return map;
	}
	
	public static JobDetail createDetail(String name, String jobGroup, Collection<Node> nodeList) {
		JobDetail detail = new JobDetail(name, jobGroup, NodeBalancingJob.class);
		detail.setJobDataMap(buildJobDataMap(detail.getJobDataMap(), nodeList));
		detail.setDurability(true);
		detail.setVolatility(false);
		detail.setRequestsRecovery(true);
		return detail;
	}
}
