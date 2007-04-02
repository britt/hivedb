package org.hivedb;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.HiveUtils;
import org.springframework.jdbc.core.support.JdbcDaoSupport;


public class HiveDataSourceCacheImpl implements HiveDataSourceCache{
	private Hive hive;
	private Map<Integer, JdbcDaoSupport> jdbcDaoSupports = new ConcurrentHashMap<Integer, JdbcDaoSupport>();
	
	private JdbcDaoSupport get(Node node, AccessType intention)
	{
		if (!jdbcDaoSupports.containsKey(node))
			jdbcDaoSupports.put(hash(node, intention), new DataNodeJdbcDaoSupport(node.getUri()));
		return jdbcDaoSupports.get(hash(node, intention));
	}
	
	private static int hash(Node node, AccessType intention) {
		return HiveUtils.makeHashCode(new Object[] {node, intention});
	}
	private static class DataNodeJdbcDaoSupport extends JdbcDaoSupport
	{
		public DataNodeJdbcDaoSupport(String databaseUri)
		{
			this.setDataSource(new HiveBasicDataSource(databaseUri));
		}
	}
	// TODO: WHat is the appropriate exception behavior?
	public JdbcDaoSupport get(PartitionDimension partitionDimension, Object primaryIndexKey, AccessType intention) throws HiveReadOnlyException {
		try {
			return get(hive.getNodeOfPrimaryIndexKey(partitionDimension, primaryIndexKey), intention);
		} catch (HiveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public JdbcDaoSupport get(SecondaryIndex secondaryIndex, Object secondaryIndexKey, AccessType intention) throws HiveReadOnlyException {
		// TODO Auto-generated method stub
		try {
			return get(hive.getNodeOfSecondaryIndexKey(secondaryIndex, secondaryIndexKey), intention);
		} catch (HiveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
