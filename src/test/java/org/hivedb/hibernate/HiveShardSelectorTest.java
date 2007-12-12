package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Types;
import java.util.Collection;

import org.hibernate.shards.ShardId;
import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
/***
 * More complex and useful tests will be added when dynamic shard configuration is added.
 * @author bcrawford
 *
 */
public class HiveShardSelectorTest extends H2HiveTestCase {
	@BeforeMethod
	public void setUp() throws Exception {
		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2));
	}

	@Test
	public void testSelectNode() throws Exception {
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		Hive hive = getHive();
		HiveShardSelector selector = new HiveShardSelector(reader.getHiveConfiguration(), hive);
		WeatherReport report = WeatherReportImpl.generate();
		
		ShardId id = selector.selectShardIdForNewObject(report);
		assertNotNull(id);
		
		Collection<Integer> nodeIds = Transform.map(new Unary<Node, Integer>(){
			public Integer f(Node n) {
				return n.getId();
			}}, hive.getNodes()); 
		
		assertTrue(Filter.grepItemAgainstList(new Integer(id.getId()), nodeIds));
	}	
}
