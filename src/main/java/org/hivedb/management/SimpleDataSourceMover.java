package org.hivedb.management;

import java.sql.Types;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.meta.Hive;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;

public abstract class SimpleDataSourceMover<T> implements Mover {
	private RowMapper rowMapper;
	private String table, idColumn;
	private int primaryIndexKeyType = Types.INTEGER;
	
	public SimpleDataSourceMover(RowMapper rowMapper, String table, String idColumn, int primaryIndexKeyType) {
		this.rowMapper = rowMapper;
		this.table = table;
		this.idColumn = idColumn;
		this.primaryIndexKeyType = primaryIndexKeyType;
	}
	
	public MoveReport move(Migration migration) throws MigrationException{
		DataSource origin = new HiveBasicDataSource(migration.getOriginUri());
		DataSource destination = new HiveBasicDataSource(migration.getDestinationUri());
		
		try {
			Collection<T> migrants = findByPrimaryIndexKey(migration.getPrimaryIndexKey(),origin);
			save(migrants, destination);
			Hive hive = Hive.load(migration.getHiveUri());
			hive.updatePrimaryIndexNode(
					migration.getPartitionDimension(), 
					migration.getPrimaryIndexKey(), 
					migration.getDestinationUri()
			);
			delete(migration.getPrimaryIndexKey(), origin);
		} catch(Exception e) {
			throw new MigrationException(e);
		}
		return null;
	}

	private void delete(Object primaryIndexKey, DataSource origin) {
		Object[] parameters = new Object[] {primaryIndexKey};
		
		JdbcTemplate j = new JdbcTemplate(origin);
		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(
				deleteByPrimaryIndexKeySql(), 
				new int[] {primaryIndexKeyType});
		j.update(factory.newPreparedStatementCreator(parameters));
	}

	private void save(Collection<T> migrants, DataSource destination) {
		for(T migrant : migrants)
			save(migrant,destination);
	}

	protected abstract void save(T migrant, DataSource destination);

	@SuppressWarnings("unchecked")
	private Collection<T> findByPrimaryIndexKey(Object primaryIndexKey, DataSource origin) {
		Object[] parameters = new Object[] {primaryIndexKey};
		
		JdbcTemplate j = new JdbcTemplate(origin);
		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(
				selectByPrimaryIndexKeySql(), 
				new int[] {primaryIndexKeyType});
	
		return j.query(factory.newPreparedStatementCreator(parameters), rowMapper);
	}

	private String selectByPrimaryIndexKeySql() {
		return "select * from "+ table +" where "+ idColumn +" = ? ";
	}
	
	private String deleteByPrimaryIndexKeySql() {
		return "delete from "+ table +" where "+ idColumn +" = ? ";
	}
	
	private String deleteFailureMessage(Object id, String nodeUri) {
		StringBuilder msg = new StringBuilder("Error occurred while deleting migrated records.  Orphan records with partition key ");
		msg.append(id);
		msg.append(" remain on node with uri ");
		msg.append(nodeUri);
		return msg.toString();
	}
}
