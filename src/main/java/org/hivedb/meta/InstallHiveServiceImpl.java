package org.hivedb.meta;

import org.apache.log4j.Logger;
import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.util.InstallHiveIndexSchema;

/**
 *  Exposable service for installing a hive.
 * @author andylikuski
 *
 * @param <T>
 * @param <N>
 */
public class InstallHiveServiceImpl implements InstallHiveService {
	private final Logger log = Logger.getLogger(InstallHiveServiceImpl.class);
	private final SingularHiveConfig hiveConfig;
	
	public InstallHiveServiceImpl(SingularHiveConfig hiveConfig) {		
		this.hiveConfig = hiveConfig;
		installIndexSchema();
	}

	private void installIndexSchema() {
		InstallHiveIndexSchema.install(hiveConfig);
	}

	public Node registerNode(String name, String dataNodeUri) {
		final Hive hive = hiveConfig.getHive();
		final PartitionDimension partitionDimension = hive.getPartitionDimension();
		
		for (Node node : partitionDimension.getNodes()) {
			if (name.equals(node.getName()))
				return node;
		}
		
		try {
			int maxId = 0;
			for (Node node : partitionDimension.getNodes())
				maxId = Math.max(maxId, node.getId());
			final Node node = new Node(maxId+1,name, name, dataNodeUri, partitionDimension.getId(), hive.getDialect());
			hive.addNode(node); 
			return node;
			
		} catch (HiveException ex) {
			throw new HiveRuntimeException("Unable to register node: "
					+ ex.getMessage(), ex);
		}
	}
}
