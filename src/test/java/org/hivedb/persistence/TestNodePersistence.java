package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;

import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcUriFormatter;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.Test;

public class TestNodePersistence extends H2HiveTestCase {	  
	@Test
	public void testCreate() throws Exception {
		NodeDao dao = new NodeDao(getDataSource(getHiveDatabaseName()));
		assertEquals(0,dao.loadAll().size());
		
		Node full = createFullyPopulatedNode();
		Node minimal = createMinimalNode();
		
		dao.create(full);
		dao.create(minimal);
		
		List<Node> nodes = dao.loadAll();
		assertEquals(2,nodes.size());	
		
		Node fetchedFull = null;
		Node fetchedMinimal = null;
		
		for(Node n : nodes)
			if(n.getName().equals(full.getName()))
				fetchedFull = n;
			else if(n.getName().equals(minimal.getName()))
				fetchedMinimal = n;
		
		assertNotNull(fetchedFull);
		assertNotNull(fetchedMinimal);
		
		assertEquals(full, fetchedFull);
		assertEquals(minimal, fetchedMinimal);
		
		assertFalse(fetchedFull.isReadOnly());
		assertFalse(fetchedMinimal.isReadOnly());
	}
	
	@Test
	public void testUpdate() {
		NodeDao dao = new NodeDao(getDataSource(getHiveDatabaseName()));
		assertEquals(0,dao.loadAll().size());
		
		Node full = createFullyPopulatedNode();
		Node minimal = createMinimalNode();
		
		dao.create(full);
		dao.create(minimal);
		
		full.setDatabaseName("notBlahDatabase");
		
		minimal.setUsername("minimus");
		minimal.setPassword("maximus");
		
		dao.update(full);
		dao.update(minimal);
		
		List<Node> nodes = dao.loadAll();
		assertEquals(2,nodes.size());	
		
		Node fetchedFull = null;
		Node fetchedMinimal = null;
		
		for(Node n : nodes)
			if(n.getName().equals(full.getName()))
				fetchedFull = n;
			else if(n.getName().equals(minimal.getName()))
				fetchedMinimal = n;
		
		assertNotNull(fetchedFull);
		assertNotNull(fetchedMinimal);
		
		assertEquals(full, fetchedFull);
		assertEquals(minimal, fetchedMinimal);
		
		assertNotNull(fetchedMinimal.getUsername());
		assertNotNull(fetchedMinimal.getPassword());
	}
	
	@Test
	public void testDelete() {
		NodeDao dao = new NodeDao(getDataSource(getHiveDatabaseName()));
		assertEquals(0,dao.loadAll().size());
		
		Node full = createFullyPopulatedNode();
		Node minimal = createMinimalNode();
		
		dao.create(full);
		dao.create(minimal);
		
		List<Node> nodes = dao.loadAll();
		assertEquals(2,nodes.size());
		for(Node n : nodes)
			dao.delete(n);
		
		assertEquals(0,dao.loadAll().size());
	}

	@Test
	public void testStringyness() {
		Node full = createFullyPopulatedNode();
		Node minimal = createMinimalNode();
		
		System.out.println(new JdbcUriFormatter(full).getUri());
		System.out.println(new JdbcUriFormatter(minimal).getUri());

	}
	
	public Node createFullyPopulatedNode() {
		Node node = createMinimalNode();
		node.setName("full node");
		node.setReadOnly(false);
		node.setUsername("test");
		node.setPassword("test");
		node.setPort(3306);
		node.setCapacity(101);
		node.setOptions("&works=true");
		return node;
	}
	
	public Node createMinimalNode() {
		return new Node(
				Hive.NEW_OBJECT_ID, 
				"minimal node", 
				"blahbase", 
				"localhost", 
				HiveDbDialect.MySql
			);
	}
}
