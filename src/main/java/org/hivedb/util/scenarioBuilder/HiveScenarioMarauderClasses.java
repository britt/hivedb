package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;

public class HiveScenarioMarauderClasses {
	public static class Pirate implements PrimaryAndSecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static int idGenerator = 0;
		public Pirate() { 
			id = ++idGenerator;
			pirateName = "name"+id; }		
		protected int id;
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		String pirateName;
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return pirateName;}
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		
		public String getPartitionDimensionName() { return this.getClass().getSimpleName(); }
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() {return "name";	}
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Pirate.class;
		}
		public Class getIdClass() { return Integer.class; }
	}
	public static class Buccaneer implements PrimaryAndSecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static int idGenerator = 0;
		public Buccaneer() { 
			id = new Integer(++idGenerator).toString();
			buccaneerName = "name"+id;};
		protected String id;
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		String buccaneerName;
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return this; }
		public String getIdAsSecondaryIndexInstance() {	return buccaneerName; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return id; } // self referencing
		
		public String getPartitionDimensionName() { return this.getClass().getSimpleName(); }
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "name";	}
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Buccaneer.class;
		}
		public Class getIdClass() { return String.class; }
	}
	
	
	// Resource and Secondary index classes that test all possible secondary index id types
	public static class Treasure implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static int idGenerator = 0;
		protected int id;
		Pirate primaryResource;
		public Treasure() {} // Prototype constructor
		public Treasure(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); } 
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() {return "pirate_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Pirate.class;
		}
		public Class getIdClass() { return Integer.class; }
	}	                      
	public static class Booty implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static long idGenerator = 2^32;
		protected long id;
		Pirate primaryResource;
		public Booty() {} // Prototype constructor
		public Booty(Pirate primaryResource) {
			id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public Long getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Long getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "pirate_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Pirate.class;
		}
		public Class getIdClass() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	public static class Loot implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static int idGenerator = 0;
		protected int id;
		Pirate primaryResource;
		public Loot() {} // Prototype constructor
		public Loot(Pirate primaryResource) {
			this.id = ++idGenerator;
			this.primaryResource = primaryResource;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public Integer getIdAsPrimaryIndexInstance() { return id; }
		public Pirate getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public Integer getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "pirate_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Pirate.class;
		}
		public Class getIdClass() { return Integer.class; }
	}
	public static class Stash implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static double idGenerator = 0d;
		protected double id;
		Buccaneer primaryResource;
		public Stash() {} // Prototype constructor
		public Stash(Buccaneer primaryResource) {
			idGenerator += 1.1;
			this.id = idGenerator;
			this.primaryResource = primaryResource;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public Double getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Double getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Buccaneer.class;
		}
		public Class getIdClass() { return Double.class; }
	}
	public static class Chanty implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{
		public static int idGenerator = 0;
		protected String id;
		Buccaneer primaryResource;
		public Chanty() {} // Prototype constructor
		public Chanty(Buccaneer primaryResource) {
			this.id = new Integer(++idGenerator).toString(); 
			this.primaryResource = primaryResource;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public String getIdAsPrimaryIndexInstance() { return id; }
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public String getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Buccaneer.class;
		}
		public Class getIdClass() { return String.class; }
	}
	public static class Bottle implements SecondaryIndexIdentifiable, ResourceIdentifiable
	{	
		static int current = 0;
		protected int id;
		Buccaneer primaryResource;
		public Bottle() {} // Prototype constructor
		public Bottle(Buccaneer primaryResource) {
			this.primaryResource = primaryResource;
			id = ++current;
		}
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		public Buccaneer getPrimaryIndexInstanceReference() { return primaryResource; }
		public Integer getIdAsSecondaryIndexInstance() { return id; }
		public String getPrimaryIndexIdAsSecondaryIndexInstance() { return primaryResource.getIdAsPrimaryIndexInstance(); }
		
		public String getResourceName() { return this.getClass().getSimpleName(); }
		public String getSecondaryIdName() { return "bucaneer_id"; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexClass() {
			return Buccaneer.class;
		}
		public Class getIdClass() { return Integer.class; }
	}
}
