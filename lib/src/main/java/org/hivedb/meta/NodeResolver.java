package org.hivedb.meta;

import java.util.Collection;

public interface NodeResolver {

	/***
	 * Get the Partition Dimension that this directory applies to. 
	 * @return 
	 */
	public PartitionDimension getPartitionDimension();

	/***
	 * Add a primary index key
	 * @param nodes The nodes to add the primary index key to.
	 * @param primaryIndexKey The key to be added
	 */
	public void insertPrimaryIndexKey(final Collection<Node> nodes, final Object primaryIndexKey);

	/***
	 * Add a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param primaryindexKey
	 */
	public void insertSecondaryIndexKey(final SecondaryIndex secondaryIndex,final Object secondaryIndexKey, Object primaryindexKey);

	/***
	 * Change the nodes of a primary index key.
	 * @param nodes
	 * @param primaryIndexKey
	 */
	public void updatePrimaryIndexKey(final Collection<Node> nodes,final Object primaryIndexKey);

	/***
	 * Set the write lock on a priamry index key.
	 * @param primaryIndexKey
	 * @param isReadOnly
	 */
	public void updatePrimaryIndexKeyReadOnly(final Object primaryIndexKey, boolean isReadOnly);

	/***
	 * Change the primary index key that a secondary index key references.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param originalPrimaryIndexKey
	 * @param newPrimaryIndexKey
	 */
	public void updatePrimaryIndexOfSecondaryKey(
			final SecondaryIndex secondaryIndex,
			final Object secondaryIndexKey,
			final Object originalPrimaryIndexKey,
			final Object newPrimaryIndexKey);

	/***
	 * Delete the secondary index keys associated with a primary index key.
	 * @param secondaryIndex
	 * @param primaryIndexKey
	 */
	public void deleteAllSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey);

	/***
	 * Delete a primary index key.
	 * @param primaryIndexKey
	 */
	public void deletePrimaryIndexKey(final Object primaryIndexKey);

	/***
	 * Delete a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 */
	public void deleteSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey, final Object primaryIndexKey);

	/***
	 * Test the existence of a primary key
	 * @param primaryIndexKey
	 * @return
	 */
	public boolean doesPrimaryIndexKeyExist(final Object primaryIndexKey);

	/***
	 * Get the ids of the nodes on which a primary keys records are stored.
	 * @param primaryIndexKey
	 * @return
	 */
	public Collection<Integer> getNodeIdsOfPrimaryIndexKey(final Object primaryIndexKey);

	/***
	 * Get (id,write-lock state) of a primary index key.
	 * @param primaryIndexKey
	 * @return
	 */
	public Collection<NodeSemaphore> getNodeSemamphoresOfPrimaryIndexKey(final Object primaryIndexKey);

	/***
	 * Get the write-lock state of a primary index key
	 * @param primaryIndexKey
	 * @return
	 */
	public boolean getReadOnlyOfPrimaryIndexKey(final Object primaryIndexKey);

	/***
	 * Test the existence of a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	public boolean doesSecondaryIndexKeyExist(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey);

	/***
	 * Get the ids of the nodes on which a secondary keys records are stored.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	public Collection<Integer> getNodeIdsOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey);

	/***
	 * Get the (id,read-lock state) of a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	public Collection<NodeSemaphore> getNodeSemaphoresOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey);

	/***
	 * Get the primary indexs that have records matching the secondary index key provided.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	public Collection<Object> getPrimaryIndexKeysOfSecondaryIndexKey(final SecondaryIndex secondaryIndex, final Object secondaryIndexKey);

	/***
	 * Get all of the keys associated with a primary index for a particular secondary index.
	 * @param secondaryIndex
	 * @param primaryIndexKey
	 * @return
	 */
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(final SecondaryIndex secondaryIndex, final Object primaryIndexKey);

}