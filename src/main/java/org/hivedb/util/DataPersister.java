package org.hivedb.util;

import java.io.Serializable;

import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.BaseDataAccessObjectFactory;
import org.hivedb.util.Persister;

public class DataPersister implements Persister {
	
	private EntityHiveConfig entityHiveConfig;
	private DataAccessObject<Object, Serializable> dataAccessObject;
	private Hive hive;
	public DataPersister(EntityHiveConfig entityHiveConfig, Class representedInterface, DataAccessObject<Object, Serializable> dataAccessObject, Hive hive)
	{
		this.entityHiveConfig = entityHiveConfig;
		this.dataAccessObject = dataAccessObject;
		this.hive = hive;
	}
	
	public Object persistPrimaryIndexKey(EntityHiveConfig entityHiveConfig, Class representedInterface, Object primaryIndexKey){
		// handled by the dataAccessObject
		return primaryIndexKey;
	}

	public Object persistResourceInstance(EntityHiveConfig entityHiveConfig, Class representedInterface, Object resourceInstance) {	
		return dataAccessObject.save(resourceInstance);
	}
	
	public Object persistSecondaryIndexKey(EntityHiveConfig entityHiveConfig, Class representedInterface, EntityIndexConfig entitySecondaryIndexConfig, Object resourceInstance){
		// all needed persistence is done in persistResourceInstance
		return entitySecondaryIndexConfig;
	}
}
