package org.hivedb.meta.directory;

import java.util.Collection;

import org.hivedb.meta.KeySemaphore;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

public interface NodeResolver {
	/***
	 * Get the Partition Dimension that this directory applies to. 
	 * @return 
	 */
	public PartitionDimension getPartitionDimension();

	
	
	/***
	 * Test the existence of a primary key
	 * @param primaryIndexKey
	 * @return
	 */
	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey);

	/***
	 * Test the existence of a resource id.
	 * @param resource
	 * @param id
	 * @return
	 */
	public boolean doesResourceIdExist(Resource resource, Object id);
	
	/***
	 * Get (id,write-lock state) of a primary index key.
	 * @param primaryIndexKey
	 * @return
	 */
	public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey);

	/***
	 * Test the existence of a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param resourceId
	 * @return
	 */
	public boolean doesSecondaryIndexKeyExist(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object resourceId);

	/***
	 * Get the (id,read-lock state) of a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey);

	/***
	 * Get the primary indexs that have records matching the secondary index key provided.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection getPrimaryIndexKeysOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey);

	/***
	 * Get the resource ids associated witha  secondary index key
	 * @param secondaryIndex
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection getResourceIdsOfSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey);
	
	/***
	 * Get all of the keys associated with a primary index for a particular secondary index.
	 * @param secondaryIndex
	 * @param primaryIndexKey
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfPrimaryIndexKey(SecondaryIndex secondaryIndex, Object primaryIndexKey);
	
	/***
	 * Get all of the keys associated with a resource id for a particular secondary index.
	 * @param resource
	 * @param id
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection getSecondaryIndexKeysOfResourceId(SecondaryIndex secondaryIndex, Object id);
	
	/***
	 * Get the primary index key of a resource.
	 * @param resource
	 * @param id
	 * @return
	 */
	public Object getPrimaryIndexKeyOfResourceId(Resource resource, Object id);
}