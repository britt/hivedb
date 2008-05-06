package org.hivedb.management.migration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.meta.Node;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.DirectoryWrapper;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.meta.persistence.TableInfo;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestMigration extends H2HiveTestCase {

	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		
		for(String name: getDatabaseNames()) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(getConnectString(name)));
			
			try {
				dao.getJdbcTemplate().update("SET DB_CLOSE_DELAY 5");
			}
			catch (Exception e) {} // only protection against multiple calls (create a Schema class to avoid this)
		}
	}
	
	// Add this test's schema to that of the superclass
	@SuppressWarnings("unchecked")
	protected Collection<Schema> getDataNodeSchemas() {
		return Transform.flatten(super.getDataNodeSchemas(),
			Transform.flatMap(new Unary<String, Collection<Schema>>() {
				public Collection<Schema> f(String dataNodeName) {
					return Arrays.asList(new Schema[] {
							new TestMigrationSchema(getConnectString(dataNodeName)),
					});
				}
			}, getDataNodeNames()));
	}
	
	@Test
	public void testMigration() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		String primaryKey = new String("Asia");
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		NodeResolver dir = new Directory(hive.getPartitionDimension(), CachingDataSourceProvider.getInstance().getDataSource(getConnectString(getHiveDatabaseName())));
		PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
		Mover<Integer> secMover = new SecondaryMover();
		
		//Do the actual migration
		Migrator m = new HiveMigrator(hive);
		assertNotNull(Filter.grepItemAgainstList(origin.getId(), Transform.map(DirectoryWrapper.semaphoreToId(),dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
		m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
		//Directory points to the destination node
		assertNotNull(Filter.grepItemAgainstList(destination.getId(), Transform.map(DirectoryWrapper.semaphoreToId(),dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
		//Records exist and are identical on the destination node
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, secMover.get(secondaryKey, destination));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFailDuringCopy() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		String primaryKey = new String("Oceana");
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		NodeResolver dir = new Directory(hive.getPartitionDimension(), CachingDataSourceProvider.getInstance().getDataSource(getConnectString(getHiveDatabaseName())));
		PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
		//This mover just craps out on copy
		Mover<Integer> failingMover = new Mover<Integer>() {
			public void copy(Integer item, Node node) {
				throw new RuntimeException("");
			}
			public void delete(Integer item, Node node) {}
			public Integer get(Object id, Node node) {return null;}
		};
		
		Migrator m = new HiveMigrator(hive);
		pMover.getDependentMovers().clear();
		pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
		try {
			m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points to the origin node
		assertNotNull(Filter.grepItemAgainstList(origin.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
		//Records are intact on the origin node
		assertEquals(primaryKey, pMover.get(primaryKey, origin));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, origin));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFailDuringDelete() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		String primaryKey = new String("Asia");
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		
		NodeResolver dir = new Directory(hive.getPartitionDimension(), getDataSource(getHiveDatabaseName()));
		PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
//		This mover just craps out on delete
		Mover<Integer> failingMover = new Mover<Integer>() {
			public void copy(Integer item, Node node) {
				SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
				dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
				dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
			}
			public void delete(Integer item, Node node) {throw new RuntimeException("Ach!");}
			public Integer get(Object id, Node node) {
				SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
				dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
				return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
			}
		};
		
		Migrator m = new HiveMigrator(hive);
		pMover.getDependentMovers().clear();
		pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
		try {
			m.migrate(primaryKey,Arrays.asList(new String[]{destination.getName()}), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points destination
		assertNotNull(Filter.grepItemAgainstList(destination.getId(), Transform.map(DirectoryWrapper.semaphoreToId(), dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey))));
		//Records exist ondestination
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, destination));
	}
	
	
	private Pair<Node, Node> initializeTestData(Hive hive, String primaryKey, Integer secondaryKey) throws Exception {
		hive.directory().insertPrimaryIndexKey(primaryKey);
		//Setup the test data on one node
		NodeResolver dir = new Directory(hive.getPartitionDimension(), getDataSource(getHiveDatabaseName()));
		int originId = Atom.getFirst(dir.getKeySemamphoresOfPrimaryIndexKey(primaryKey)).getId();
		Node origin = hive.getNode(originId);
		Node destination = origin.getName().equals("data1") ? hive.getNode("data2") : 
			hive.getNode("data1");
		PartitionKeyMover<String> pMover = new PrimaryMover(origin.getUri());
		Mover<Integer> secMover = new SecondaryMover();
		pMover.copy(primaryKey, origin);
		secMover.copy(secondaryKey, origin);
		return new Pair<Node, Node>(origin, destination);
	}
	
	private class TestMigrationSchema extends Schema {

		public TestMigrationSchema(String uri) {
			super("testMigration", uri);
		}

		@Override
		public Collection<TableInfo> getTables() {
			return Arrays.asList(
				new TableInfo("primary_table", "create table primary_table (id varchar(50));"),
				new TableInfo("secondary_table", "create table secondary_table (id integer);"));
		}
	}
	
	class PrimaryMover implements PartitionKeyMover<String> {
		@SuppressWarnings("unchecked")
		private Collection<Entry<Mover, KeyLocator>> movers;
		private String originUri;
		
		@SuppressWarnings("unchecked")
		public PrimaryMover(String uri) {
			this.originUri = uri;
			movers =  new ArrayList<Entry<Mover,KeyLocator>>();
			movers.add(new Pair<Mover, KeyLocator>(new SecondaryMover(), new SecondaryKeyLocator(originUri)) );
		}
		
		@SuppressWarnings("unchecked")
		public Collection<Entry<Mover, KeyLocator>> getDependentMovers() {
			return movers;
		}

		public void copy(String item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			dao.getJdbcTemplate().update("insert into primary_table values (?)", new Object[]{item});
		}

		public void delete(String item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			dao.getJdbcTemplate().update("delete from primary_table where id = ?", new Object[]{item});
		}

		public String get(Object id, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			return (String) dao.getJdbcTemplate().queryForObject("select id from primary_table where id = ?", new Object[]{id}, String.class);
		}
		
	}
	
	class SecondaryKeyLocator implements KeyLocator<String, Integer> {
		private String uri;
		
		public SecondaryKeyLocator(String uri) {
			this.uri = uri;
		}
		
		@SuppressWarnings("unchecked")
		public Collection<Integer> findAll(String parent) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(uri));
			return dao.getJdbcTemplate().queryForList("select id from secondary_table", Integer.class);
		}
		
	}
	
	class SecondaryMover implements Mover<Integer> {

		public void copy(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
		}

		public void delete(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			dao.getJdbcTemplate().update("delete from secondary_table where id = ?", new Object[]{item});
		}

		public Integer get(Object id, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(CachingDataSourceProvider.getInstance().getDataSource(node.getUri()));
			return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
		}
		
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{getHiveDatabaseName(), "data1","data2"});
	}
	
	@Override
	public String getHiveDatabaseName() {
		return "hive";
	}
}
