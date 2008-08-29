package org.hivedb.hibernate;

import org.hibernate.shards.ShardId;
import org.hivedb.Hive;
import org.hivedb.meta.Node;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.junit.Test;
import org.junit.Assert;import static org.junit.Assert.assertTrue;

import java.util.Collection;

/***
 * More complex and useful tests will be added when dynamic shard configuration is added.
 * @author bcrawford
 *
 */
@Config(file="hive_default")
public class HiveShardSelectorTest extends HiveTest {

	@Test
	public void testSelectNode() throws Exception {
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		Hive hive = getHive();
		HiveShardSelector selector = new HiveShardSelector(reader.getHiveConfiguration(), hive);
		WeatherReport report = WeatherReportImpl.generate();
		
		ShardId id = selector.selectShardIdForNewObject(report);
		Assert.assertNotNull(id);
		
		Collection<Integer> nodeIds = Transform.map(new Unary<Node, Integer>(){
			public Integer f(Node n) {
				return n.getId();
			}}, hive.getNodes()); 
		
		assertTrue(Filter.grepItemAgainstList(new Integer(id.getId()), nodeIds));
	}	
}
