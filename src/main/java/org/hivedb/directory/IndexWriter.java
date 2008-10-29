package org.hivedb.directory;

import org.hivedb.Node;
import org.hivedb.Resource;
import org.hivedb.SecondaryIndex;

public interface IndexWriter {
	public void insertPrimaryIndexKey(Node node, Object primaryIndexKey);

	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly);
	
	public void deletePrimaryIndexKey(Object primaryIndexKey);
	
	public void insertSecondaryIndexKey(SecondaryIndex secondaryIndex,Object secondaryIndexKey, Object primaryindexKey);

	public void deleteSecondaryIndexKey(SecondaryIndex secondaryIndex, Object secondaryIndexKey, Object primaryIndexKey);

    public void insertResourceId(Resource resource, Object id, Object primaryIndexKey);

	public void updatePrimaryIndexKeyOfResourceId(
			Resource resource, 
			Object resourceId, 
			Object newPrimaryIndexKey);

    public void deleteResourceId(Resource resource, Object id);
	

}
