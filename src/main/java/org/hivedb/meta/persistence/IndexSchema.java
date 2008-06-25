/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import org.hivedb.Schema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.database.Schemas;

import java.util.Collection;

/**
 * IndexSchema contains tables of primary and secondary indexes in
 * accordance with the rows existing in the Global Hive meta tables.
 * Each IndexSchema instance references a particular jdbc URI where it will
 * create index tables. All primary and secondary indexes of a partition
 * index must be stored at the same URI, hence you should always construct
 * and IndexSchema with the URI of a partition dimension's index node.
 * <p>
 * 
 * @author Andy Likuski (alikuski@cafepress.com)
 * @author Britt Crawford (bcrawford@cafepress.com)
 */
public class IndexSchema extends Schema {
	private PartitionDimension partitionDimension;
	
	/**
	 * IndexSchema is constructed against a JDBC URI, which will be the destination
	 * for the schema tables.
	 * 
	 * @param dbURI Empty target database connect string, including username, password & catalog
	 * @param dialect Data definition language dialect
	 */
	public IndexSchema(PartitionDimension partitionDimension) {
		super("Hive index schema");
		this.partitionDimension = partitionDimension;
	}

	public PartitionDimension getPartitionDimension() {
		return partitionDimension;
	}

	@Override
	public Collection<TableInfo> getTables(String uri) {
		return Schemas.getTables(partitionDimension);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IndexSchema) {
			return super.equals(obj) && partitionDimension.equals(((IndexSchema) obj).getPartitionDimension());
		}
		return false;
	}
}
