package org.hivedb.management.migration;

import java.util.List;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.functional.Pair;

public class HiveMigrator implements Migrator {
	private Hive hive;
	private PartitionDimension dimension;
	
	public HiveMigrator(Hive hive, String dimensionName) {
		this.hive = hive;
		this.dimension = hive.getPartitionDimension(dimensionName);
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.migration.Migrator#migrate(java.lang.Object, java.lang.String, java.lang.String, org.hivedb.management.migration.PartitionKeyMover)
	 */
	@SuppressWarnings("unchecked")
	public void migrate(Object key, String originName, String destinationName, PartitionKeyMover mover) {
		Node origin = getNode(originName);
		Node destination = getNode(destinationName);
		
		lock(key); //Lock the partition key for writing
		Object migrant = mover.get(key,origin);
		
		try {
			//Copy the Partition Key Instance
			mover.copy(migrant, destination);
			//Copy all dependent records
			for(Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()){
				for(Object childKey : p.getValue().findAll(migrant)) {
					Mover childMover = p.getKey();
					Object child = childMover.get(childKey, origin);
					childMover.copy(child, destination);
				}
			}
			//Update the partition key location
			hive.updatePrimaryIndexNode(dimension, key, destination);
		} catch (HiveException e) {
			throw new MigrationException(
					String.format("Failed to update directory entry for %s. Records may be orphaned on node %s", 
							key, 
							destination.getName()), e);
		} catch( RuntimeException e) {
			throw new MigrationException(
					String.format("An error occured while copying records from node % to node %s.  Records may be orphaned on node %s",
							destination.getName(),
							origin.getName(), 
							destination.getName()), e);
		} finally {
			//Always unlock the record even if the move fails.  The records haven't been deleted yet so the originals should still be there.
			unlock(key);
		}
		
		//Delete dependent records first, just in case there are FKs.
		for(Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()){
			for(Object childKey : p.getValue().findAll(migrant)) {
				Mover childMover = p.getKey();
				Object child = childMover.get(childKey, origin);
				childMover.delete(child, origin);
			}
		}
		//Delete the primary index instance.
		mover.delete(migrant, origin);
	}
	
	private Node getNode(String id) {
		return dimension.getNodeGroup().getNode(id);
	}
	
	private void lock(Object key) {
		try {
			hive.updatePrimaryIndexReadOnly(dimension, key, true);
		} catch (HiveException e) {
			throw new MigrationException("Failed to lock partition key "+ key +" for writing.", e);
		}
	}
	
	private void unlock(Object key) {
		try {
			hive.updatePrimaryIndexReadOnly(dimension, key, false);
		} catch (HiveException e) {
			throw new MigrationException("Failed to unlock partition key " + key + " for writing.", e);
		}
	}
}
