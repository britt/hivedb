/**
 * 
 */
package org.hivedb.util.scenarioBuilder;

import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;

public class HiveScenarioMarauderConfig implements HiveScenarioConfig {
	
	private Hive hive;
	public HiveScenarioMarauderConfig(String connectString) {
		try {
			try {
				new GlobalSchema(connectString).install();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			BasicDataSource ds = new HiveBasicDataSource(connectString);
			new HiveSemaphoreDao(ds).create();
			hive = Hive.load(connectString, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public Hive getHive() {
		return hive;
	}
	
	public int getInstanceCountPerPrimaryIndex() { return 10; }
	public int getInstanceCountPerSecondaryIndex() { return 100; };
	// Classes to be used as primary indexes
	public  Class[] getPrimaryClasses() { return new Class[] { Pirate.class, Buccaneer.class };}
	// Classes to be used as resources and secondary indexes.
	// If the classes are also primary indexes, then the secondary index created will be
	// a property of class, such as name, which will reference the id of the class (an intra-class reference.)
	// If the classes are no also primary classes, then the secondary index created will be
	// the class's id which references the id of another class (an inter-class reference)
	public Class[] getResourceAndSecondaryIndexClasses() {
		return  new Class[] {
			Pirate.class, Buccaneer.class, Treasure.class, Booty.class, Loot.class, Stash.class, Chanty.class, Bottle.class};
	}
	
	public Collection<String> getIndexUris(final Hive hive) {
		return Generate.create(new Generator<String>(){
			public String f() { return hive.getHiveUri(); }}, new NumberIterator(2));
	}
		// The nodes of representing the data storage databases. These may be nonunique as well.
	public Collection<Node> getNodes(final Hive hive) {
		return Generate.create(new Generator<Node>(){
			public Node f() { return new Node(  hive.getHiveUri(), 										
												false); }},
							  new NumberIterator(3));
	}
	
	private static class Pirate implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Pirate() { 
			id = ++idGenerator;
			pirateName = "name"+id; }		
		protected int id;
		String pirateName;
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return pirateName;}
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		
		public String getPartitionDimensionName() { return this.getClass().getSimpleName(); }
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() {return "name";	}
	}
	private static class Buccaneer implements PrimaryAndSecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Buccaneer() { 
			id = new Integer(++idGenerator).toString();
			buccaneerName = "name"+id;};
		protected String id;
		String buccaneerName;
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return buccaneerName; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		
		public String getPartitionDimensionName() { return this.getClass().getSimpleName(); }
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "name";	}
	}
	
	
	// Resource and Secondary index classes that test all possible secondary index id types
	private static class Treasure implements SecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		protected int id;
		Pirate primaryResource;
		public Treasure(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); } 
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() {return "pirate_id"; }
	}	                      
	private static class Booty implements SecondaryIndexIdentifiable
	{
		public static long idGenerator = 2^32;
		protected long id;
		Pirate primaryResource;
		public Booty(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Long getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Long getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "pirate_id"; }
	}
	private static class Loot implements SecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		protected int id;
		Pirate primaryResource;
		public Loot(Pirate primaryResource) {
			this.id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "pirate_id"; }
	}
	private static class Stash implements SecondaryIndexIdentifiable
	{
		public static double idGenerator = 0d;
		protected double id;
		Buccaneer primaryResource;
		public Stash(Buccaneer primaryResource) {
			idGenerator += 1.1;
			this.id = idGenerator;
			this.primaryResource = primaryResource;
		}
		public Double getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Double getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
	}
	private static class Chanty implements SecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		protected String id;
		Buccaneer primaryResource;
		public Chanty(Buccaneer primaryResource) {
			this.id = new Integer(++idGenerator).toString(); 
			this.primaryResource = primaryResource;
		}
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public String getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
	}
	private static class Bottle implements SecondaryIndexIdentifiable
	{	
		static int current = 0;
		protected int id;
		Buccaneer primaryResource;

		public Bottle(Buccaneer primaryResource) {
			this.primaryResource = primaryResource;
			id = ++current;
		}
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
	}
}