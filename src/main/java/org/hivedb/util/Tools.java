/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 * 
 * @author Kevin Kelm (kkelm@fortress-consulting.com)
 */
package org.hivedb.util;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.command.AddPartitionDimensionCommand;
import org.hivedb.meta.command.CreateHiveCommand;

/**
 * Command-line tool implemented with a GNU Getopt port.
 * 
 * @author Justin McCarthy (jmccarthy@cafepress.com)
 */
public class Tools {
	public static void main(String[] args) throws HiveException {
		int choice;
		String hiveUri = System.getProperty("hiveuri");
		Getopt gg = new Getopt("Tools", args, "", getOptions());

		if (args.length == 0) {
			printUsage();
		} else
			while ((choice = gg.getopt()) != -1) {
				switch (choice) {
				case 'c':
					CreateHiveCommand cmd = new CreateHiveCommand();
					cmd.setJdbcUri(hiveUri);
					cmd.execute(Hive.load(hiveUri));
					break;
				case 'd':
					System.out
							.println("[add partition dimension unimplemented]");
					break;
				}
			}
	}

	private static LongOpt[] getOptions() {
		LongOpt[] lopts = {
				new LongOpt("create", LongOpt.OPTIONAL_ARGUMENT, null, 'c'),
				new LongOpt("create-dimension", LongOpt.OPTIONAL_ARGUMENT,
						null, 'd') };
		return lopts;
	}

	private static void printUsage() {
		System.out.println("Usage: Tools -Dhiveuri=<hive uri> [command]\n");
		System.out.println(getHelp());
	}

	private static String getHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append("create\t\t\t\t\tInstall the Hive schema\n");
		sb
				.append("create-dimension <name:jdbctype>\tAdd a new partition dimesion of type "
						+ AddPartitionDimensionCommand
								.getDelimitedJdbcTypeList() + "\n");
		return sb.toString();
	}
}
