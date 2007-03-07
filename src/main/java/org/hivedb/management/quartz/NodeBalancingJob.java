package org.hivedb.management.quartz;

import java.util.Calendar;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.management.ConfigurableEstimator;
import org.hivedb.management.Migration;
import org.hivedb.management.MigrationEstimator;
import org.hivedb.management.MigrationPlanningException;
import org.hivedb.management.NodeBalancer;
import org.hivedb.management.OverFillBalancer;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerUtils;

/**
 * A naieve nodebalancing implementation.  It will get smarter as 
 * the code evolves but right now its "balance as fast as you can".
 * @author britt
 *
 */
public class NodeBalancingJob implements Job {
	public static final char JOB_NAME_SEPARATOR = '.';
	public static final String NODE_LIST_KEY = "Nodes";
	private static final String DIMENSION_KEY = "PartitionDimension";
	private static final String DIRECTORY_URI_KEY = "DirectoryUri";
	
	private PartitionKeyStatisticsDao statsDao;

	private PartitionDimension dimension;
	private NodeBalancer balancer;
	private MigrationEstimator estimator;
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getMergedJobDataMap();
		dimension = (PartitionDimension) map.get(DIMENSION_KEY);
		Collection<Node> nodes = (Collection<Node>) map.get(NODE_LIST_KEY);
		DataSource ds = new HiveBasicDataSource(map.get(DIRECTORY_URI_KEY).toString());
		estimator = ConfigurableEstimator.getInstance();
		balancer = new OverFillBalancer(dimension, estimator, ds);
		
		try {
			Collection<Migration> movePlan = balancer.suggestMoves(nodes);
			enqueueMoves(movePlan, context.getScheduler());
		} catch( MigrationPlanningException e) {
			throw new JobExecutionException(e);
		}
	}
	
	private void enqueueMoves(Collection<Migration> migrations, Scheduler scheduler) throws JobExecutionException {
		long delay = 0;
		for(Migration migration : migrations){
			PartitionKeyStatistics stats = statsDao.findByPrimaryPartitionKey(dimension, migration.getMigrantId());
			long executionTime = estimator.estimateMoveTime(stats);
			// TODO Better delay computation
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
	
	private static JobDataMap buildJobDataMap(JobDataMap map, Collection<Node> nodeList, PartitionDimension dimension, String directoryUri) {
		map.put(DIRECTORY_URI_KEY, directoryUri);
		map.put(NODE_LIST_KEY, nodeList);
		map.put(DIMENSION_KEY, dimension);
		return map;
	}
	
	public static JobDetail createDetail(String name, String jobGroup, Collection<Node> nodeList, PartitionDimension dimension, String directoryUri) {
		JobDetail detail = new JobDetail(name, jobGroup, NodeBalancingJob.class);
		detail.setJobDataMap(buildJobDataMap(detail.getJobDataMap(), nodeList, dimension, directoryUri));
		detail.setDurability(true);
		detail.setVolatility(false);
		detail.setRequestsRecovery(true);
		return detail;
	}
}
