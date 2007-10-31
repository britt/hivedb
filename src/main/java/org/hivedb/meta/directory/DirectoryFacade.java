package org.hivedb.meta.directory;

import java.util.Collection;

import org.hivedb.HiveReadOnlyException;
import org.hivedb.meta.KeySemaphore;

public interface DirectoryFacade {

	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey);
	public Collection<Integer> getNodeIdsOfPrimaryIndexKey(Object primaryIndexKey);
	public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey);
	public Collection getResourceIdsOfPrimaryIndexKey(String resource,Object primaryIndexKey);
	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey);
	public void insertPrimaryIndexKey(Object primaryIndexKey) throws HiveReadOnlyException;
	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) throws HiveReadOnlyException;
	public void deletePrimaryIndexKey(Object primaryIndexKey) throws HiveReadOnlyException;

	public boolean doesSecondaryIndexKeyExist(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId);
	public Collection<Integer> getNodeIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public void insertSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException;
	public void deleteSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveReadOnlyException;

	public boolean doesResourceIdExist(String resource, Object resourceId);
	public Collection<Integer> getNodeIdsOfResourceId(String resource, Object id);
	public Collection<KeySemaphore> getKeySemaphoresOfResourceId(String resource, Object resourceId);
	public boolean getReadOnlyOfResourceId(String resource, Object id);
	public void insertResourceId(String resource, Object id, Object primaryIndexKey) throws HiveReadOnlyException;
	public void updatePrimaryIndexKeyOfResourceId(String resource, Object resourceId, Object newPrimaryIndexKey) throws HiveReadOnlyException;
	public void deleteResourceId(String resource, Object id) throws HiveReadOnlyException;
	
	public Collection getSecondaryIndexKeysWithResourceId(String resource, String secondaryIndex, Object id);
	public Collection getPrimaryIndexKeysOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Collection getResourceIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Object getPrimaryIndexKeyOfResourceId(String name, Object resourceId);

}