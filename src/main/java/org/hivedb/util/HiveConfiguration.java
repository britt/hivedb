package org.hivedb.util;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Finder;
import org.hivedb.meta.Nameable;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.scenarioBuilder.Filter;
import org.hivedb.util.scenarioBuilder.Pair;
import org.hivedb.util.scenarioBuilder.Transform;
import org.hivedb.util.scenarioBuilder.Unary;

public class HiveConfiguration extends Hive {

	protected HiveConfiguration(String hiveUri, int revision, boolean readOnly, Collection<PartitionDimension> partitionDimensions, PartitionKeyStatisticsDao statistics) {
		super(hiveUri, revision, readOnly, partitionDimensions, statistics);
	}
	public HiveConfiguration(Hive hive)
	{
		super(hive.getHiveUri(), hive.getRevision(), hive.isReadOnly(), hive.getPartitionDimensions(), hive.getPartitionStatistics());
	}

	/**
	 *  Updates the hive at the given uri to be in sync with this hive configurations
	 * @param hiveUri
	 * @return the synced hive
	 */
	public HiveDelta syncHive(String hiveUri)
	{	
		// Add missing partition dimensions
		final Finder live;
		try {
			live = Hive.load(hiveUri);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
		final Finder updater =  this;
		
		Collection<PartitionDimension> missingPartitionDimensions = getMissingItems(PartitionDimension.class, live, updater);
		
		Collection<Entry<PartitionDimension, PartitionDimension>> partitionDimensionPairs =	getLikeNamedInstances(PartitionDimension.class, live, updater);
		// Add missing nodes of each existing partion dimension
		Map<PartitionDimension, Collection<Node>> missingNodesOfExistingPartitionDimension = getMissingItems(Node.class, partitionDimensionPairs);
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
							live.findByName(PartitionDimension.class, partitionDimension.getName()), 
							updater.findByName(PartitionDimension.class, partitionDimension.getName()));
					return getMissingItems(SecondaryIndex.class, resourcePairs);
					} catch (Exception e) { throw new RuntimeException(e); }
			}},
			partitionDimensionsWithoutMissingResources);
		
		new HiveDelta(missingPartitionDimensions,
					missingNodesOfExistingPartitionDimension,  
					missingResourcesOfExistingPartitionDimension,
					missingSecondaryIndexesOfExistingResources );
		
		/*
		Transform.digMap(
			new Ternary<PartitionDimension, Resource, SecondaryIndex, SecondaryIndex>
				(PartitionDimension partionDimension, Resource resource, SecondaryIndex secondaryIndex) {
		
		}},
		missingSecondaryIndexesOfExistingResources);
		*/
		return null;
	}

	public <T extends Nameable> Collection<Entry<T, T>> getLikeNamedInstances(final Class<T> ofClass, final Finder live, final Finder updater) {
		return Transform.map(
				new Unary<String, Entry<T, T>>() {
					public Entry<T, T> f(String name) {
						try {
							return new Pair<T, T>(
									(T)live.findByName(ofClass, name),
									(T)updater.findByName(ofClass, name)); 
						} catch (HiveException e) { throw new RuntimeException(e); }}},	
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
		return Filter.grepFalseAgainstList(live.findCollection(ofClass), updater.findCollection(ofClass));
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
		return Filter.grepAgainstList(live.findCollection(ofClass), updater.findCollection(ofClass));
	}
	
	private<T extends Nameable> Collection<String> getNames(Collection<T> collection)
	{
		return Transform.map(new Unary<T, String>() {
			public String f(T identifiable) { return identifiable.getName();}},
			collection);
	}
}
