package org.hivedb.management.quartz;

import org.hivedb.management.HivePersistable;
import org.hivedb.management.Mover;
import org.hivedb.management.MoverFactory;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Moves a record from one node of the hive to another.
 * @author Britt Crawford (bcrawford@cafepress.com)
 *
 */
public class MigrationJob implements Job {
	public static final String MIGRATION_KEY = "Migration";
	
	// TODO How is this injected?
	private MoverFactory factory;
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getMergedJobDataMap();
		Migration migration = (Migration) map.get(MIGRATION_KEY);
		Mover<HivePersistable> mover = factory.getMoverForClass(migration.getMigrantId().getClass());
		
		//This call should definitely have some checked exceptions
		mover.move(
				migration, 
				getRate());
		//Movers are responsible for controlling their own rate which makes me nervous, 
		//but every other solution is hard.
	}
	
	private int getRate() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private static JobDataMap buildJobDataMap(JobDataMap map, Migration migration) {
		map.put(MIGRATION_KEY, migration);
		return map;
	}
	
	public static JobDetail createDetail(String name, String jobGroup, Migration migration) {
		JobDetail detail = new JobDetail(name, jobGroup, MigrationJob.class);
		detail.setJobDataMap(buildJobDataMap(detail.getJobDataMap(), migration));
		detail.setDurability(false);
		detail.setVolatility(false);
		detail.setRequestsRecovery(true);
		return detail;
	}
}
