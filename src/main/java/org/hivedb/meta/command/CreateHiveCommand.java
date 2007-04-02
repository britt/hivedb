package org.hivedb.meta.command;

import gnu.getopt.LongOpt;

import org.hivedb.Hive;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.GlobalSchema;

/**
 * @author Justin McCarthy (jmccarty@cafepress.com)
 */
public class CreateHiveCommand extends HiveCommand {
	public static final String ARGUMENT_NAME = "create";
	public static final char ARGUMENT_ID = 1;

	public void execute(Hive aHive) {
		GlobalSchema hiveSchema;
		try {
			hiveSchema = new GlobalSchema(getJdbcUri());
			hiveSchema.install();
		} catch (Exception e) {
			throw new HiveRuntimeException("Unable to install schema at " + getJdbcUri() + ": " + e.getMessage(),e);
		}
	}

	public static LongOpt createOption() {
		return new LongOpt(ARGUMENT_NAME, LongOpt.REQUIRED_ARGUMENT, null, ARGUMENT_ID);
	}
}