package org.hivedb.management.migration;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.directory.DbDirectory;
import org.hivedb.meta.directory.KeySemaphore;
import org.hivedb.util.Lists;
import org.hivedb.util.functional.Collect;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

import java.util.Collection;
import java.util.List;

public class HiveMigrator implements Migrator {
  private Hive hive;
  private PartitionDimension dimension;

  public HiveMigrator(Hive hive) {
    this.hive = hive;
    this.dimension = hive.getPartitionDimension();
  }

  @SuppressWarnings("unchecked")
  public void deepNodeToNodeCopy(Object migrant, Node origin, Node destination, PartitionKeyMover mover) {
    try {
      //Copy the Partition Key Instance
      mover.copy(migrant, destination);
      //Copy all dependent records
      for (Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()) {
        final KeyLocator value = p.getValue();
        for (Object childKey : value.findAll(migrant)) {
          Mover childMover = p.getKey();
          Object child = childMover.get(childKey, origin);
          childMover.copy(child, destination);
        }
      }
    } catch (RuntimeException e) {
      throw new MigrationException(
        String.format("An error occured while copying records from node %s to node %s.  Records may be orphaned on node %s",
          destination.getName(),
          origin.getName(),
          destination.getName()), e);
    }
  }

  @SuppressWarnings("unchecked")
  public void cascadeDelete(Object migrant, Node node, PartitionKeyMover mover) {
    //Delete dependent records first, just in case there are FKs.
    for (Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()) {
      for (Object childKey : p.getValue().findAll(migrant)) {
        Mover childMover = p.getKey();
        Object child = childMover.get(childKey, node);
        childMover.delete(child, node);
      }
    }
    //Delete the primary index instance.
    mover.delete(migrant, node);
  }

  /* (non-Javadoc)
    * @see org.hivedb.management.migration.Migrator#migrate(java.lang.Object, java.lang.String, java.lang.String, org.hivedb.management.migration.PartitionKeyMover)
    */
  @SuppressWarnings("unchecked")
  public void move(Object key, Node origin, Node destination, PartitionKeyMover mover) {
    Object migrant = mover.get(key, origin);

    try {
      //Copy the Partition Key Instance
      mover.copy(migrant, destination);
      //Copy all dependent records
      for (Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()) {
        for (Object childKey : p.getValue().findAll(migrant)) {
          Mover childMover = p.getKey();
          Object child = childMover.get(childKey, origin);
          childMover.copy(child, destination);
        }
      }
      //Update the partition key location
//			hive.updatePrimaryIndexNode(dimension, key, destination);
//		} catch (HiveException e) {
//			throw new MigrationException(
//					String.format("Failed to update directory entry for %s. Records may be orphaned on node %s", 
//							key, 
//							destination.getName()), e);
    } catch (RuntimeException e) {
      throw new MigrationException(
        String.format("An error occured while copying records from node %s to node %s.  Records may be orphaned on node %s",
          destination.getName(),
          origin.getName(),
          destination.getName()), e);
    }

    //Delete dependent records first, just in case there are FKs.
    for (Pair<Mover, KeyLocator> p : (List<Pair<Mover, KeyLocator>>) mover.getDependentMovers()) {
      for (Object childKey : p.getValue().findAll(migrant)) {
        Mover childMover = p.getKey();
        Object child = childMover.get(childKey, origin);
        childMover.delete(child, origin);
      }
    }
    //Delete the primary index instance.
    mover.delete(migrant, origin);
  }

  private Node getNode(int id) {
    return hive.getNode(id);
  }

  private Node getNode(String id) {
    return hive.getNode(id);
  }

  private void lock(Object key) {
    try {
      hive.directory().updatePrimaryIndexKeyReadOnly(key, true);
    } catch (HiveLockableException e) {
      throw new MigrationException("Failed to lock partition key " + key + " for writing.", e);
    }
  }

  private void unlock(Object key) {
    try {
      hive.directory().updatePrimaryIndexKeyReadOnly(key, false);
    } catch (HiveLockableException e) {
      throw new MigrationException("Failed to unlock partition key " + key + " for writing.", e);
    }
  }

  public void migrate(Object key, Collection<String> destinationNames, PartitionKeyMover mover) {
    Collection<Node> destinations = Collect.amass(new Unary<String, Node>() {

      public Node f(String item) {
        return getNode(item);
      }
    }, destinationNames);
    doMigration(key, destinations, mover);
  }

  private void doMigration(Object key, Collection<Node> destinations, PartitionKeyMover mover) {
    try {
      lock(key);
      DbDirectory dir = new DbDirectory(dimension);
      Collection<Node> origins = Transform.map(new Unary<KeySemaphore, Node>() {
        public Node f(KeySemaphore keySemaphore) {
          return getNode(keySemaphore.getNodeId());
        }
      }, dir.getKeySemamphoresOfPrimaryIndexKey(key));

      //Elect a random origin node as the authority
      Node authority = Lists.random(origins);
      Object migrant = mover.get(key, authority);

      //Copy the records
      for (Node destination : destinations) {
        try {
          deepNodeToNodeCopy(migrant, authority, destination, mover);
        } catch (RuntimeException e) {
          throw new MigrationException(String.format("Error while copying records to node %s", destination.getName()), e);
        }
      }
      //Update the directory entries
      try {
        dir.deletePrimaryIndexKey(key);
        for (Node destination : destinations)
          dir.insertPrimaryIndexKey(destination, key);
      } catch (RuntimeException e) {
        try {
          //try to repair the damage
          for (Node origin : origins)
            dir.insertPrimaryIndexKey(origin, key);
        } catch (Exception ex) {
        }
        throw new MigrationException(
          String.format("Failed to update directory entry for %s. Records may be orphaned.",
            key), e);
      }

      for (Node node : origins) {
        try {
          cascadeDelete(migrant, node, mover);
        }
        catch (RuntimeException e) {
          throw new MigrationException(String.format("Error deleting old records on node %s", node.getName()), e);
        }
      }

    } finally {
      unlock(key);
    }
  }
}
