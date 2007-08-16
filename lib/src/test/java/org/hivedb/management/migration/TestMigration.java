package org.hivedb.management.migration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.directory.Directory;
import org.hivedb.meta.directory.NodeResolver;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Pair;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestMigration extends H2HiveTestCase {
	private Hive hive;

	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		try {
			hive = Hive.load(getConnectString(getHiveDatabaseName()));
			hive.addPartitionDimension(createPopulatedPartitionDimension());
			new IndexSchema(hive.getPartitionDimension(partitionDimensionName())).install();
			for(String name : getDatabaseNames())
				hive.addNode(hive.getPartitionDimension(partitionDimensionName()), new Node(name, getConnectString(name)));
			
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		
		for(String name: getDatabaseNames()) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(getConnectString(name)));
			
			dao.getJdbcTemplate().update(createPrimaryTableSql());
			dao.getJdbcTemplate().update(createSecondaryTableSql());
			dao.getJdbcTemplate().update("SET DB_CLOSE_DELAY 5");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testMigration() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		NodeResolver dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString(getHiveDatabaseName())));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
		Mover<Integer> secMover = new SecondaryMover();
		
		//Do the actual migration
		Migrator m = new HiveMigrator(hive, partitionDimensionName());
		assertNotNull(Filter.grepItemAgainstList(origin.getId(), dir.getNodeIdsOfPrimaryIndexKey(primaryKey)));
		m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
		//Directory points to the destination node
		assertNotNull(Filter.grepItemAgainstList(destination.getId(), dir.getNodeIdsOfPrimaryIndexKey(primaryKey)));
		//Records exist and are identical on the destination node
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, secMover.get(secondaryKey, destination));
	}
	
	@Test
	public void testFailDuringCopy() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		NodeResolver dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString(getHiveDatabaseName())));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
		//This mover just craps out on copy
		Mover<Integer> failingMover = new Mover<Integer>() {
			public void copy(Integer item, Node node) {
				throw new RuntimeException("");
			}
			public void delete(Integer item, Node node) {}
			public Integer get(Object id, Node node) {return null;}
		};
		
		Migrator m = new HiveMigrator(hive, partitionDimensionName());
		pMover.getDependentMovers().clear();
		pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
		try {
			m.migrate(primaryKey, Arrays.asList(new String[]{destination.getName()}), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points to the origin node
		assertNotNull(Filter.grepItemAgainstList(origin.getId(), dir.getNodeIdsOfPrimaryIndexKey(primaryKey)));
		//Records are intact on the origin node
		assertEquals(primaryKey, pMover.get(primaryKey, origin));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, origin));
	}
	
	@Test
	public void testFailDuringDelete() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		
		NodeResolver dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), getDataSource(getHiveDatabaseName()));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
//		This mover just craps out on delete
		Mover<Integer> failingMover = new Mover<Integer>() {
			public void copy(Integer item, Node node) {
				SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
				dao.setDataSource(new HiveBasicDataSource(node.getUri()));
				dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
			}
			public void delete(Integer item, Node node) {throw new RuntimeException("Ach!");}
			public Integer get(Object id, Node node) {
				SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
				dao.setDataSource(new HiveBasicDataSource(node.getUri()));
				return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
			}
		};
		
		Migrator m = new HiveMigrator(hive, partitionDimensionName());
		pMover.getDependentMovers().clear();
		pMover.getDependentMovers().add(new Pair<Mover, KeyLocator>(failingMover, new SecondaryKeyLocator(origin.getUri())));
		try {
			m.migrate(primaryKey,Arrays.asList(new String[]{destination.getName()}), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points destination
		assertNotNull(Filter.grepItemAgainstList(destination.getId(), dir.getNodeIdsOfPrimaryIndexKey(primaryKey)));
		//Records exist ondestination
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, destination));
	}
	
	
	private Pair<Node, Node> initializeTestData(Hive hive, Integer primaryKey, Integer secondaryKey) throws Exception {
		hive.insertPrimaryIndexKey(partitionDimensionName(), primaryKey);
		//Setup the test data on one node
		NodeResolver dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), getDataSource(getHiveDatabaseName()));
		int originId = Atom.getFirst(dir.getNodeIdsOfPrimaryIndexKey(primaryKey));
		Node origin = hive.getPartitionDimension(partitionDimensionName()).getNode(originId);
		Node destination = origin.getName().equals("data1") ? hive.getPartitionDimension(partitionDimensionName()).getNode("data2") : 
			hive.getPartitionDimension(partitionDimensionName()).getNode("data1");
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
		Mover<Integer> secMover = new SecondaryMover();
		pMover.copy(primaryKey, origin);
		secMover.copy(secondaryKey, origin);
		return new Pair<Node, Node>(origin, destination);
	}
	
	private String createPrimaryTableSql() {
		return "create table primary_table (id integer)";
	}
	
	private String createSecondaryTableSql() {
		return "create table secondary_table  (id integer)";
	}
	
	class PrimaryMover implements PartitionKeyMover<Integer> {
		private List<Pair<Mover, KeyLocator>> movers;
		private String originUri;
		
		public PrimaryMover(String uri) {
			this.originUri = uri;
			movers =  new ArrayList<Pair<Mover,KeyLocator>>();
			movers.add(new Pair<Mover, KeyLocator>(new SecondaryMover(), new SecondaryKeyLocator(originUri)) );
		}
		
		public List<Pair<Mover, KeyLocator>> getDependentMovers() {
			return movers;
		}

		public void copy(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			dao.getJdbcTemplate().update("insert into primary_table values (?)", new Object[]{item});
		}

		public void delete(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			dao.getJdbcTemplate().update("delete from primary_table where id = ?", new Object[]{item});
		}

		public Integer get(Object id, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			return dao.getJdbcTemplate().queryForInt("select id from primary_table where id = ?", new Object[]{id});
		}
		
	}
	
	class SecondaryKeyLocator implements KeyLocator<Integer, Integer> {
		private String uri;
		
		public SecondaryKeyLocator(String uri) {
			this.uri = uri;
		}
		
		@SuppressWarnings("unchecked")
		public Collection<Integer> findAll(Integer parent) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(uri));
			return dao.getJdbcTemplate().queryForList("select id from secondary_table", Integer.class);
		}
		
	}
	
	class SecondaryMover implements Mover<Integer> {

		public void copy(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
		}

		public void delete(Integer item, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			dao.getJdbcTemplate().update("delete from secondary_table where id = ?", new Object[]{item});
		}

		public Integer get(Object id, Node node) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(node.getUri()));
			return dao.getJdbcTemplate().queryForInt("select id from secondary_table where id = ?", new Object[]{id});
		}
		
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{getHiveDatabaseName(), "data1","data2"});
	}
	
	@Override
	protected String getHiveDatabaseName() {
		return "hive";
	}
}
