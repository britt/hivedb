package org.hivedb.meta.directory;

import java.util.Collection;
import java.util.Map;

import org.hivedb.HiveLockableException;
import org.hivedb.meta.KeySemaphore;

public interface DirectoryFacade {

	public boolean doesPrimaryIndexKeyExist(Object primaryIndexKey);
	public Collection<Integer> getNodeIdsOfPrimaryIndexKey(Object primaryIndexKey);
	public Collection<KeySemaphore> getKeySemamphoresOfPrimaryIndexKey(Object primaryIndexKey);
	public Collection getResourceIdsOfPrimaryIndexKey(String resource,Object primaryIndexKey);
	public boolean getReadOnlyOfPrimaryIndexKey(Object primaryIndexKey);
	public void insertPrimaryIndexKey(Object primaryIndexKey) throws HiveLockableException;
	public void updatePrimaryIndexKeyReadOnly(Object primaryIndexKey, boolean isReadOnly) throws HiveLockableException;
	public void deletePrimaryIndexKey(Object primaryIndexKey) throws HiveLockableException;

	public boolean doesSecondaryIndexKeyExist(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId);
	public Collection<Integer> getNodeIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Collection<KeySemaphore> getKeySemaphoresOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public void insertSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveLockableException;
	public void deleteSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey, Object resourceId) throws HiveLockableException;

	public boolean doesResourceIdExist(String resource, Object resourceId);
	public Collection<Integer> getNodeIdsOfResourceId(String resource, Object id);
	public Collection<KeySemaphore> getKeySemaphoresOfResourceId(String resource, Object resourceId);
	public boolean getReadOnlyOfResourceId(String resource, Object id);
	public void insertResourceId(String resource, Object id, Object primaryIndexKey) throws HiveLockableException;
	public void updatePrimaryIndexKeyOfResourceId(String resource, Object resourceId, Object newPrimaryIndexKey) throws HiveLockableException;
	public void deleteResourceId(String resource, Object id) throws HiveLockableException;
	
	public Collection getSecondaryIndexKeysWithResourceId(String resource, String secondaryIndex, Object id);
	public Collection getPrimaryIndexKeysOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Collection getResourceIdsOfSecondaryIndexKey(String resource, String secondaryIndex, Object secondaryIndexKey);
	public Object getPrimaryIndexKeyOfResourceId(String name, Object resourceId);
	
	public void deleteAllSecondaryIndexKeysOfResourceId(String resource, Object id) throws HiveLockableException;
	public void deleteSecondaryIndexKeys(String resource, Map<String, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveLockableException;
	public void insertSecondaryIndexKeys(String resource, Map<String, Collection<Object>> secondaryIndexValueMap, final Object resourceId) throws HiveLockableException;
	
}