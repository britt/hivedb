package org.hivedb.management.migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hivedb.Hive;
import org.hivedb.HiveException;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Directory;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.DerbyUtils;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Pair;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

public class TestMigration extends HiveTestCase {
	
	public TestMigration() {
		this.cleanupDbAfterEachTest = true;
		this.databaseNames = Arrays.asList(new String[] {getDatabaseName(), "data1", "data2"});
	}
	
	@BeforeMethod
	public void setUp() throws Exception {
		super.setUp();
		Hive hive = Hive.load(getConnectString());
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		new IndexSchema(hive.getPartitionDimension(partitionDimensionName())).install();
		for(String name : this.databaseNames)
			hive.addNode(hive.getPartitionDimension(partitionDimensionName()), new Node(name, DerbyUtils.connectString(name)));
		
		for(String name: this.databaseNames) {
			SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
			dao.setDataSource(new HiveBasicDataSource(DerbyUtils.connectString(name)));
			
			dao.getJdbcTemplate().update(createPrimaryTableSql());
			dao.getJdbcTemplate().update(createSecondaryTableSql());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testMigration() throws Exception {
		Hive hive = Hive.load(getConnectString());
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		Directory dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString()));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
		Mover<Integer> secMover = new SecondaryMover();
		
		//Do the actual migration
		Migrator m = new HiveMigrator(hive, partitionDimensionName());
		assertEquals(origin.getId(), dir.getNodeIdOfPrimaryIndexKey(primaryKey));
		m.migrate(primaryKey, origin.getName(), destination.getName(), pMover);
		//Directory points to the destination node
		assertEquals(destination.getId(), dir.getNodeIdOfPrimaryIndexKey(primaryKey));
		//Records exist and are identical on the destination node
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, secMover.get(secondaryKey, destination));
	}
	
	@Test
	public void testFailDuringCopy() throws Exception {
		Hive hive = Hive.load(getConnectString());
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		Directory dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString()));
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
			m.migrate(primaryKey, origin.getName(), destination.getName(), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points to the origin node
		assertEquals(origin.getId(), dir.getNodeIdOfPrimaryIndexKey(primaryKey));
		//Records are intact on the origin node
		assertEquals(primaryKey, pMover.get(primaryKey, origin));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, origin));
	}
	
	@Test
	public void testFailDuringDirectoryUpdate() throws Exception {
		Hive hive = Hive.load(getConnectString());
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		Directory dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString()));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
		
		Migrator m = new HiveMigrator(new NoUpdateHive(hive.getHiveUri(), 1, false, hive.getPartitionDimensions(), null), partitionDimensionName());
		try {
			m.migrate(primaryKey, origin.getName(), destination.getName(), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points to origin
		assertEquals(origin.getId(), dir.getNodeIdOfPrimaryIndexKey(primaryKey));
		//Records exist on both nodes
		assertEquals(primaryKey, pMover.get(primaryKey, origin));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, origin));
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, destination));
	}
	
	@Test
	public void testFailDuringDelete() throws Exception {
		Hive hive = Hive.load(getConnectString());
		Integer primaryKey = new Integer(2);
		Integer secondaryKey = new Integer(7);
		
		Pair<Node, Node> nodes = initializeTestData(hive, primaryKey, secondaryKey);
		Node origin = nodes.getKey();
		Node destination = nodes.getValue();
		Directory dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString()));
		PartitionKeyMover<Integer> pMover = new PrimaryMover(origin.getUri());
//		This mover just craps out on delete
		Mover<Integer> failingMover = new Mover<Integer>() {
			public void copy(Integer item, Node node) {
				SimpleJdbcDaoSupport dao = new SimpleJdbcDaoSupport();
				dao.setDataSource(new HiveBasicDataSource(node.getUri()));
				dao.getJdbcTemplate().update("insert into secondary_table values (?)", new Object[]{item});
			}
			public void delete(Integer item, Node node) {throw new RuntimeException("");}
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
			m.migrate(primaryKey, origin.getName(), destination.getName(), pMover);
		} catch( Exception e) {
			//Quash
		}
		//Directory still points destination
		assertEquals(destination.getId(), dir.getNodeIdOfPrimaryIndexKey(primaryKey));
		//Records exist ondestination
		assertEquals(primaryKey, pMover.get(primaryKey, destination));
		assertEquals(secondaryKey, new SecondaryMover().get(secondaryKey, destination));
	}
	
	
	private Pair<Node, Node> initializeTestData(Hive hive, Integer primaryKey, Integer secondaryKey) throws HiveException, SQLException {
		hive.insertPrimaryIndexKey(hive.getPartitionDimension(partitionDimensionName()), primaryKey);
		//Setup the test data on one node
		Directory dir = new Directory(hive.getPartitionDimension(partitionDimensionName()), new HiveBasicDataSource(getConnectString()));
		int originId = dir.getNodeIdOfPrimaryIndexKey(primaryKey);
		Node origin = hive.getPartitionDimension(partitionDimensionName()).getNodeGroup().getNode(originId);
		Node destination = origin.getName().equals("data1") ? hive.getPartitionDimension(partitionDimensionName()).getNodeGroup().getNode("data2") : 
			hive.getPartitionDimension(partitionDimensionName()).getNodeGroup().getNode("data1");
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

	class NoUpdateHive extends Hive {
		
		protected NoUpdateHive(String hiveUri, int revision, boolean readOnly, Collection<PartitionDimension> partitionDimensions, PartitionKeyStatisticsDao statistics) {
			super(hiveUri, revision, readOnly, partitionDimensions, statistics);
		}
		
		@Override
		public void updatePrimaryIndexNode(PartitionDimension partitionDimension,
				Object primaryIndexKey, Node node) throws HiveException {
			throw new HiveException("");
		}
		
	}
}
