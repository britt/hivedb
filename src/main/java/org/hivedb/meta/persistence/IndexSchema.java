/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.meta.persistence;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.velocity.context.Context;
import org.hivedb.Schema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.Templater;
import org.hivedb.util.database.JdbcTypeMapper;

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
public class IndexSchema extends Schema{
	private PartitionDimension partitionDimension;
	
	/**
	 * IndexSchema is constructed against a JDBC URI, which will be the destination
	 * for the schema tables.
	 * 
	 * @param dbURI Empty target database connect string, including username, password & catalog
	 * @param dialect Data definition language dialect
	 */
	public IndexSchema(PartitionDimension partitionDimension) {
		super("Hive index schema",partitionDimension.getIndexUri());
		this.partitionDimension = partitionDimension;
	}
	
	protected String getCreatePrimaryIndex() {
		Context context = getContext();
		context.put("tableName", getPrimaryIndexTableName(partitionDimension));
		context.put("indexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType())));
		return Templater.render("sql/primary_index.vsql", context);
	}
	
	protected String getCreateSecondaryIndex(SecondaryIndex secondaryIndex) {
		Context context = getContext();
		context.put("tableName", getSecondaryIndexTableName(secondaryIndex));
		context.put("indexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType())));
		context.put("resourceType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getResource().getColumnType())));
		return Templater.render("sql/secondary_index.vsql", context);
	}
	
	protected String getCreateResourceIndex(Resource resource) {
		Context context = getContext();
		context.put("tableName", getResourceIndexTableName(resource));
		context.put("indexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(resource.getIdIndex().getColumnInfo().getColumnType())));
		context.put("primaryIndexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(resource.getPartitionDimension().getColumnType())));
		return Templater.render("sql/resource_index.vsql", context);
	}
	
	/**
	 * Constructs the name of the table for the primary index.
	 * @return
	 */	
	public static String getPrimaryIndexTableName(PartitionDimension partitionDimension) {
		return "hive_primary_" + partitionDimension.getName().toLowerCase();
	}
	/**
	 * Constructs the name of the table for the secondary index.
	 * @return
	 */
	public static String getSecondaryIndexTableName(SecondaryIndex secondaryIndex) {
		return "hive_secondary_" + secondaryIndex.getResource().getName().toLowerCase() + "_" + secondaryIndex.getColumnInfo().getName();	
	}
	/**
	 * Constructs the name of the table for the resource index.
	 * @return
	 */
	public static String getResourceIndexTableName(Resource resource) {
		return "hive_resource_" + resource.getName().toLowerCase();	
	}
	
	@Override
	public Collection<TableInfo> getTables() {
		Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
		TableInfos.add(new TableInfo(getPrimaryIndexTableName(partitionDimension), getCreatePrimaryIndex()));
		for (Resource resource : partitionDimension.getResources()) {
			if (!resource.isPartitioningResource())
				TableInfos.add(new TableInfo(getResourceIndexTableName(resource), getCreateResourceIndex(resource)));
			for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes())
				TableInfos.add(new TableInfo(
						getSecondaryIndexTableName(secondaryIndex), 
						getCreateSecondaryIndex(secondaryIndex)));
		}
		return TableInfos;
	}
}
