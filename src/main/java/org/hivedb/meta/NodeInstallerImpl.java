package org.hivedb.meta;

import org.apache.log4j.Logger;
import org.hivedb.HiveException;
import org.hivedb.HiveFacade;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.util.InstallHiveIndexSchema;

/**
 *  Exposable service for installing a hive.
 * @author andylikuski
 *
 * @param <T>
 * @param <N>
 */
public class NodeInstallerImpl implements NodeInstaller {
	private final Logger log = Logger.getLogger(NodeInstallerImpl.class);
	private final EntityHiveConfig entityHiveConfig;
	
	public NodeInstallerImpl(EntityHiveConfig entityHiveConfig) {		
		this.entityHiveConfig = entityHiveConfig;
		//installIndexSchema();
	}

	// Using Configuration Reader's installer instead
	//private void installIndexSchema() {
		//InstallHiveIndexSchema.install(entityHiveConfig);
	//}

	public Node registerNode(String name, String dataNodeUri) {
		final HiveFacade hive = entityHiveConfig.getHive();
		final PartitionDimension partitionDimension = hive.getPartitionDimension();
		
		for (Node node : hive.getNodes()) {
			if (name.equals(node.getName()))
				return node;
		}
		
		try {
			int maxId = 0;
			for (Node node : hive.getNodes())
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
