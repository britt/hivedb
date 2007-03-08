package org.hivedb.util.scenarioBuilder;

public class HiveScenarioMarauderClasses {
	public static class Pirate implements PrimaryAndSecondaryIndexIdentifiable
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
	public static class Buccaneer implements PrimaryAndSecondaryIndexIdentifiable
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
	public static class Treasure implements SecondaryIndexIdentifiable
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
	public static class Booty implements SecondaryIndexIdentifiable
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
	public static class Loot implements SecondaryIndexIdentifiable
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
	public static class Stash implements SecondaryIndexIdentifiable
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
	public static class Chanty implements SecondaryIndexIdentifiable
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
	public static class Bottle implements SecondaryIndexIdentifiable
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
