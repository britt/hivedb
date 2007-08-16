package org.hivedb.meta.directory;

import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

public interface IndexWriter {
	/***
	 * Add a primary index key
	 * @param nodes The nodes to add the primary index key to.
	 * @param primaryIndexKey The key to be added
	 */
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey);

	/***
	 * Set the write lock on a priamry index key.
	 * @param primaryIndexKey
	 * @param isReadOnly
	 */
	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly);
	
	/***
	 * Delete a primary index key.
	 * @param primaryIndexKey
	 */
	public void deletePrimaryIndexKey(Object primaryIndexKey);
	
	/***
	 * Add a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param primaryindexKey
	 */
	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,Object secondaryIndexKey, Object primaryindexKey);

	/***
	 * Delete a secondary index key
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 */
	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryIndexKey);

	/***
	 * Add a new resource key.
	 * @param resource
	 * @param id
	 */
	public void insertResourceId(Resource resource, Object id, Object primaryIndexKey);
	
	/***
	 * Change the primary index key that a resource id references.
	 * @param secondaryIndex
	 * @param secondaryIndexKey
	 * @param originalPrimaryIndexKey
	 * @param newPrimaryIndexKey
	 */
	public void updatePrimaryIndexKeyOfResourceId(
			Resource resource, 
			Object resourceId, 
			Object originalPrimaryIndexKey, 
			Object newPrimaryIndexKey);

	/***
	 * Delete a resource id.
	 * @param resource
	 * @param id
	 */
	public void deleteResourceId(Resource resource, Object id);
	

}
