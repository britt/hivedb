/**
 * HiveDB is an Open Source (LGPL) system for creating large, high-transaction-volume
 * data storage systems.
 */
package org.hivedb.util.database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;
import org.hivedb.Schema;
import org.hivedb.Schema.TrueRowMapper;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.Templater;
import org.hivedb.util.database.DialectTools;
import org.hivedb.util.database.DriverLoader;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;

/**
 * Utility class for installing and removing Schemas
 * 
 * @author mellwanger
 */
public class Schemas {
	
	private static Map<String, Set<Schema>> dataSchemas = new HashMap<String, Set<Schema>>();
	private static Map<String, IndexSchema> indexSchemas = new HashMap<String, IndexSchema>();
	
	public static Context getContext(String uri) {
		Context context = new VelocityContext();
		context.put("dialect", DriverLoader.discernDialect(uri));
		for (HiveDbDialect d : HiveDbDialect.values()) {
			context.put(DialectTools.dialectToString(d).toLowerCase(), d);
		}
		context.put("booleanType", DialectTools.getBooleanTypeForDialect(DriverLoader.discernDialect(uri)));
		context.put("sequenceModifier", DialectTools.getNumericPrimaryKeySequenceModifier(DriverLoader.discernDialect(uri)));
		return context;
	}
	
	public static String getCreatePrimaryIndex(PartitionDimension partitionDimension) {
		Context context = getContext(partitionDimension.getIndexUri());
		context.put("tableName", getPrimaryIndexTableName(partitionDimension));
		context.put("indexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(partitionDimension.getColumnType())));
		return Templater.render("sql/primary_index.vsql", context);
	}
	
	public static String getCreateSecondaryIndex(SecondaryIndex secondaryIndex, PartitionDimension partitionDimension) {
		Context context = getContext(partitionDimension.getIndexUri());
		context.put("tableName", getSecondaryIndexTableName(secondaryIndex));
		context.put("indexType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getColumnInfo().getColumnType())));
		context.put("resourceType", addLengthForVarchar(JdbcTypeMapper.jdbcTypeToString(secondaryIndex.getResource().getColumnType())));
		return Templater.render("sql/secondary_index.vsql", context);
	}
	
	public static String getCreateResourceIndex(Resource resource, PartitionDimension partitionDimension) {
		Context context = getContext(partitionDimension.getIndexUri());
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
	
	public static Collection<TableInfo> getTables(PartitionDimension partitionDimension) {
		Collection<TableInfo> TableInfos = new ArrayList<TableInfo>();
		TableInfos.add(new TableInfo(getPrimaryIndexTableName(partitionDimension), getCreatePrimaryIndex(partitionDimension)));
		for (Resource resource : partitionDimension.getResources()) {
			if (!resource.isPartitioningResource())
				TableInfos.add(new TableInfo(getResourceIndexTableName(resource), getCreateResourceIndex(resource, partitionDimension)));
			for (SecondaryIndex secondaryIndex : resource.getSecondaryIndexes())
				TableInfos.add(new TableInfo(
						getSecondaryIndexTableName(secondaryIndex), 
						getCreateSecondaryIndex(secondaryIndex, partitionDimension)));
		}
		return TableInfos;
	}
	
	public static String addLengthForVarchar(String type) {
		if (type.equals("VARCHAR")) {
			return "VARCHAR(255)";
		}
		return type;
	}
	
	public static String ifMySql(String sql, HiveDbDialect dialect) {
		return (dialect.equals(HiveDbDialect.MySql) ? sql : "");
	}
	
	
	public static void install(Schema schema, String uri) {
		for (TableInfo table : schema.getTables(uri)) {
			createTable(table, uri);
		}
		if (dataSchemas.get(uri) == null) {
			dataSchemas.put(uri, new HashSet<Schema>());
		}
		dataSchemas.get(uri).add(schema);
	}
	
	public static void uninstall(Schema schema, String uri) {
		for (TableInfo table : schema.getTables(uri)) {
			emptyTable(table, uri);
		}
		if (dataSchemas.get(uri) != null) {
			dataSchemas.get(uri).remove(schema);
		}
	}
	
	public static void install(PartitionDimension partitionDimension) {
		for (TableInfo table : getTables(partitionDimension)) {
			createTable(table, partitionDimension.getIndexUri());
		}
		if (indexSchemas.get(partitionDimension.getIndexUri()) == null) {
			indexSchemas.put(partitionDimension.getIndexUri(), new IndexSchema(partitionDimension));
		}
	}
	
	public static void uninstall(PartitionDimension partitionDimension) {
		for (TableInfo table : getTables(partitionDimension)) {
			emptyTable(table, partitionDimension.getIndexUri());
		}
		indexSchemas.remove(partitionDimension.getIndexUri());
	}
	
	public static Collection<Schema> getDataSchemas(String uri) {
		return dataSchemas.get(uri) == null ? new HashSet<Schema>() : Collections.unmodifiableCollection(new HashSet<Schema>(dataSchemas.get(uri)));
	}
	
	public static IndexSchema getIndexSchema(String uri) {
		return indexSchemas.get(uri);
	}
	
	/**
	 * Test if a table exists by trying to select from it.
	 * @param conn
	 * @param tableName
	 * @return
	 */
	public static boolean tableExists(String tableName, String uri) {
		JdbcTemplate t = new JdbcTemplate(CachingDataSourceProvider.getInstance().getDataSource(uri));
		try {
			t.query( "select * from " + tableName + ifMySql(" LIMIT 1",DriverLoader.discernDialect(uri)), new TrueRowMapper());
			return true;
		}
		catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * Conditionally create a table using the statement provided if it does
	 * not already exist.
	 * @param conn
	 * @param createStatement
	 * @throws SQLException
	 */
	public static void createTable(TableInfo table, String uri) {
		JdbcTemplate t = new JdbcTemplate(CachingDataSourceProvider.getInstance().getDataSource(uri));
		if (! tableExists(table.getName(), uri)) {
			final String createStatement = table.getCreateStatement();
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					createStatement);
			t.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
		}
	}
	
	public static void emptyTables(Schema schema, String uri) {
		for (TableInfo table : schema.getTables(uri)) {
			emptyTable(table, uri);
		}
	}
	
	public static void emptyTable(TableInfo table, String uri) {
		JdbcTemplate t = new JdbcTemplate(CachingDataSourceProvider.getInstance().getDataSource(uri));
		if (tableExists(table.getName(), uri)) {
			final String createStatement = table.getDeleteAllStatement();
			PreparedStatementCreatorFactory creatorFactory = new PreparedStatementCreatorFactory(
					createStatement);
			t.update(creatorFactory.newPreparedStatementCreator(new Object[] {}));
		}
	}
}
