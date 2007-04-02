package org.hivedb.meta.command;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.JdbcTypeMapper;

/**
 * @author Justin McCarthy (jmccarty@cafepress.com)
 */
public class AddPartitionDimensionCommand extends HiveCommand {
	private PartitionDimension newPartition = null;

	public AddPartitionDimensionCommand(String name, int jdbcType) {
		newPartition = new PartitionDimension(name, jdbcType);
	}
	
	public static String getDelimitedJdbcTypeList() {
		return JdbcTypeMapper.BIGINT + ","
		+ JdbcTypeMapper.CHAR + ","
		+ JdbcTypeMapper.DATE + ","
		+ JdbcTypeMapper.DOUBLE + ","
		+ JdbcTypeMapper.FLOAT + ","
		+ JdbcTypeMapper.INTEGER + ","
		+ JdbcTypeMapper.SMALLINT + ","
		+ JdbcTypeMapper.TIMESTAMP + ","
		+ JdbcTypeMapper.TINYINT + ","
		+ JdbcTypeMapper.VARCHAR;
	}

	public void execute(Hive aHive) {
		Hive h;
		try {
			h = Hive.load(getJdbcUri());
		} catch (HiveException e) {
			throw new HiveRuntimeException("Unable to load Hive: " + e.getMessage(),e);
		}
		
		try {
			h.addPartitionDimension(newPartition);
		} catch (HiveException e) {
			throw new HiveRuntimeException("Unable to add partition: " + e.getMessage(),e);
		}
	}
}