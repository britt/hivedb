package org.hivedb.util;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.functional.Binary;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Maps;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Ternary;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.functional.Filter.BinaryPredicate;

/**
 * HiveUpdater updates a hive to the configuration of a HiveScenarioConfig. When the HiveScenarioConfig's 
 * configuration of its primary index, resources, and secondary indexes differ from that of its Hive
 * instance, it will update the referenced hive to match its configuration. This includes name changes,
 * and the addition or removal of resources and secondary indexes.
 * 
 * @author Andy Likuski
 * TODO rename HiveUpdater
 */
public class HiveSyncer {

	private HiveFinder hiveFinder;
	private Hive hive;
	public HiveSyncer(Hive hive)
	{
		this.hive = hive;
		this.hiveFinder = new HiveFinder(hive);
	}
	/**
	 *  Sync a Hive to the configuration of the given HiveScenarioConfig. This is an additive process only:
	 *  If the Hive contains Partition Dimensions, Resources, Secondary Indexes, or Data Nodes not
	 *  specified by the HiveScenarioConfig they will be left alone, not deleted.
	 * @param entityHiveConfig
	 * @return
	 */
	public HiveDiff syncHive(EntityHiveConfig entityHiveConfig)
	{
		HiveDiff hiveDiff = diffHive(
			new HiveConfigFinder(entityHiveConfig));
		
		// Add missing secondary indexes
		Maps.digMapToCollection(new Ternary<PartitionDimension, Resource, SecondaryIndex, Void>() {
			public Void f(PartitionDimension partitionDimension, Resource resource, SecondaryIndex secondaryIndex) {
				try {
					hive.addSecondaryIndex(resource, secondaryIndex);
				} catch (HiveException e) {
					throw new HiveRuntimeException(String.format("Unable to add secondary index %s to resource %s of partition dimesnion %s",
							partitionDimension.getName(),
							resource.getName(),
							secondaryIndex.getName()));
				}
				return null;
			}
		}, hiveDiff.getMissingSecondaryIndexesOfExistingResources());
		
		// Add missing resources
		Maps.digMapToCollection(new Binary<PartitionDimension, Resource, Object>() {
			
			public Void f(PartitionDimension partitionDimension, Resource resource) {
				try {
					hive.addResource(resource);
				} catch (HiveException e) {
					throw new HiveRuntimeException(String.format("Unable to add resource %s to partition dimesnion %s",
							partitionDimension.getName(),
							resource.getName()), e);
				}
				return null;
			}
		}, hiveDiff.getMissingResourcesOfExistingPartitionDimension());
		
		// Add missing nodes
		Maps.digMapToCollection(new Binary<Hive, Node, Void>() {
			public Void f(Hive hive, Node node) {
				try {
					hive.addNode(node);
				} catch (HiveException e) {
					throw new HiveRuntimeException(String.format("Unable to add Node %s to Hive",
							node.getName()), e);
				}
				return null;
			}
		}, hiveDiff.getMissingNodesOfExistingPartitionDimension());
		
//		// Add missing partition dimensions
//		Transform.map(new Unary<PartitionDimension, Void>() {
//				public Void f(PartitionDimension partitionDimension) {
//					try {
//						hive.addPartitionDimension(partitionDimension);
//					} catch (HiveException e) {
//						throw new HiveRuntimeException(String.format("Unable to add partition dimension %s to the hive",
//								partitionDimension.getName()), e);
//					}
//					return null;
//				}
//		},	hiveDiff.getMissingPartitionDimensions());
		
		return hiveDiff;
	}

	/**
	 *  Returns a HiveDiff the difference between the HiveScenarioConfig instance and the current hive
	 * @param hiveConfig Config file file for the given hive
	 * @return a HiveDiff class that describes what partition dimensions are missing, what resources and nodes are missing
	 * from existing partion dimensions, and what secondary indexes are missing from existing resources
	 */
	public HiveDiff diffHive(final HiveConfigFinder updater)
	{	
		Collection<PartitionDimension> missingPartitionDimensions = getMissingItems(PartitionDimension.class, hiveFinder, updater);
		
		Collection<Entry<PartitionDimension, PartitionDimension>> partitionDimensionPairs =	getLikeNamedInstances(PartitionDimension.class, hiveFinder, updater);
		// Add missing nodes of each existing partion dimension 
		Collection<Entry<Hive, Hive>> hivePairs = getLikeNamedInstances(Hive.class, hiveFinder, updater);
		Map<Hive, Collection<Node>> missingNodesOfExistingPartitionDimension = getMissingItems(Node.class, hivePairs);
		// Add missing resources of each existing partion dimension
		Map<PartitionDimension, Collection<Resource>> missingResourcesOfExistingPartitionDimension = getMissingItems(Resource.class, partitionDimensionPairs);
		Collection<PartitionDimension> partitionDimensionsWithoutMissingResources
			= getExistingItems(Resource.class, partitionDimensionPairs).keySet();
		// Add missing secondary indexes of each resource
		Map<PartitionDimension, Map<Resource, Collection<SecondaryIndex>>> missingSecondaryIndexesOfExistingResources =
			Transform.toMap(
				new Transform.IdentityFunction<PartitionDimension>(),
				new Unary<PartitionDimension, Map<Resource, Collection<SecondaryIndex>> >() {
					public Map<Resource, Collection<SecondaryIndex>> f(PartitionDimension partitionDimension) {
						try {
							Collection<Entry<Resource, Resource>> resourcePairs = getLikeNamedInstances(
									Resource.class, 
									hiveFinder.findByName(PartitionDimension.class, partitionDimension.getName()), 
									updater.findByName(PartitionDimension.class, partitionDimension.getName()));
							return getMissingItems(SecondaryIndex.class, resourcePairs);
						} catch (Exception e) { throw new RuntimeException(e); }
				}},
				partitionDimensionsWithoutMissingResources);
		
		return new HiveDiff(missingPartitionDimensions,
					missingNodesOfExistingPartitionDimension,  
					missingResourcesOfExistingPartitionDimension,
					missingSecondaryIndexesOfExistingResources );
	}

	public <T extends Nameable> Collection<Entry<T, T>> getLikeNamedInstances(final Class<T> ofClass, final Finder live, final Finder updater) {
		return Transform.map(
				new Unary<String, Entry<T, T>>() {
					@SuppressWarnings("unchecked")
					public Entry<T, T> f(String name) {
						return new Pair<T, T>(
								(T)live.findByName(ofClass, name),
								(T)updater.findByName(ofClass, name));}},	
				Filter.grepAgainstList(getNames(live.findCollection(ofClass)), getNames(updater.findCollection(ofClass))));
	}

	public <C extends Finder,R extends Nameable> Map<C,Collection<R>> getMissingItems(final Class<R> ofClass, Collection<Entry<C, C>> pairs) {
		return
			Transform.toMap(
					new Transform.MapToKeyFunction<C,C>(),
					new Unary<Entry<C, C>, Collection<R>>() { public Collection<R> f(Entry<C, C> pair) {
						return getMissingItems(ofClass, pair.getKey(), pair.getValue());
					}},
					pairs);
	}
	private<C extends Finder,R extends Nameable> Collection<R> getMissingItems(final Class<R> ofClass, C live, C updater) {
		return Filter.grepFalseAgainstList(
				live.findCollection(ofClass),
				updater.findCollection(ofClass), 
				new NameableBinaryPredicate<R>());
	}
	public <C extends Finder,R extends Nameable> Map<C,Collection<R>> getExistingItems(final Class<R> ofClass, Collection<Entry<C, C>> pairs) {
		return
			Transform.toMap(
					new Transform.MapToKeyFunction<C,C>(),
					new Unary<Entry<C, C>, Collection<R>>() { public Collection<R> f(Entry<C, C> pair) {
						return getExistingItems(ofClass, pair.getKey(), pair.getValue());
					}},
					pairs);
	}
	public<C extends Finder,R extends Nameable> Collection<R> getExistingItems(final Class<R> ofClass, C live, C updater) {
		return Filter.grepAgainstList(
				live.findCollection(ofClass), 
				updater.findCollection(ofClass),
				new NameableBinaryPredicate<R>());
	}
	
	private<T extends Nameable> Collection<String> getNames(Collection<T> collection)
	{
		return Transform.map(new Unary<T, String>() {
			public String f(T identifiable) { return identifiable.getName();}},
			collection);
	}
	private class NameableBinaryPredicate<T extends Nameable> extends BinaryPredicate<T, T>
	{
		public boolean f(T item1, T item2) {
			return item1.getName().equals(item2.getName());
		}
	}
}
