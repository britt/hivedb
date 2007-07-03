package org.hivedb.management.migration;

import java.util.Collection;

import org.hivedb.meta.Directory;
import org.hivedb.meta.NodeResolver;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;

public class SecondaryIndexKeyLocator<PARENT_KEY, CHILD_KEY> implements KeyLocator<PARENT_KEY, CHILD_KEY> {
	private SecondaryIndex index; 
	private NodeResolver directory;
	
	public SecondaryIndexKeyLocator(SecondaryIndex index) {
		this.index = index;
		this.directory = new Directory(
				index.getResource().getPartitionDimension(),
				new HiveBasicDataSource(index.getResource().getPartitionDimension().getIndexUri()));
	}
	
	@SuppressWarnings("unchecked")
	public Collection<CHILD_KEY> findAll(PARENT_KEY parent) {
		return (Collection<CHILD_KEY>) directory.getSecondaryIndexKeysOfPrimaryIndexKey(index, parent);
	}
}
