package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HiveScenarioMarauderClasses {
	public static class Pirate implements PrimaryIndexIdentifiable, ResourceIdentifiable, SecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Pirate() { 
			id = ++idGenerator;
			pirateName = "name"+id; }		
		protected int id;
		String pirateName;
		public String getSecondaryIndexKey() {	return pirateName;}
		
		/* PrimaryIndexIdentifiable METHODS */
		
		// construct from a prototype instance
		public PrimaryIndexIdentifiable construct() {
			return new Pirate();
		}
		
		public Collection<ResourceIdentifiable> getResourceIdentifiables()
		{
			List<ResourceIdentifiable> list = new ArrayList<ResourceIdentifiable>();
			list.add(new Treasure(this));
			return list;
		}
		
		public Integer getPrimaryIndexKey() { return id; }
		public String getPartitionDimensionName() { return this.getClass().getSimpleName().toLowerCase(); }
		
		/* ResourceIdentifiable METHODS */
		
		// Because Pirate is also a PrimaryIndexIdentifiable the argument is equal to this.
		public ResourceIdentifiable construct(PrimaryIndexIdentifiable primaryIndexIdentifiable) {
			
			if (this != primaryIndexIdentifiable)
				throw new RuntimeException("Excpected equality here");
			return this;
		}
		
		// Because Pirate is also a SecondaryIndexIdentifiable we just return this as our only SecondaryIndexIdentifiable
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		
		public Pirate getPrimaryIndexIdentifiable() { return this; }
		public Class<? extends PrimaryIndexIdentifiable> getPrimaryIndexIdentifiableClass() {
			return Pirate.class;
		}
		
		public String getResourceName() { return this.getClass().getSimpleName().toLowerCase(); }
		
		/* SecondaryIndex METHODS */
		
		// Since Pirate is also a ResourceIdentifiable the argument is equal to this
		public SecondaryIndexIdentifiable construct(ResourceIdentifiable resourceIdentifiable) {
			if (this != resourceIdentifiable)
				throw new RuntimeException("Excpected equality here");
			return this;
		}
		
		public ResourceIdentifiable getResourceIdentifiable() {
			return this;
		}
		
		public String getSecondaryIndexColumnName() {
			return "name"; // since we're mapping Pirate.name to Pirate.id
		}
		
		// Our Secondary Index maps pirate's name to it's id;
		public String getSecondaryIndexName() {
			return getResourceName() + "." + getSecondaryIndexColumnName();
		}
	}
	
	public static class Treasure implements ResourceIdentifiable, SecondaryIndexIdentifiable 
	{
		public static int idGenerator = 0;
		protected int id;
		Pirate pirate;

		public Treasure(Pirate pirate) {
			id = ++idGenerator;
			this.pirate = pirate;
		}
		
		/* ResourceIdentifiable Methods */
	
		public ResourceIdentifiable construct(PrimaryIndexIdentifiable pirate) {
			return new Treasure((Pirate)pirate);
		}
		
		public Pirate getPrimaryIndexIdentifiable() { return pirate; }
		public String getResourceName() { return this.getClass().getSimpleName().toLowerCase(); }
	
		// Since Treasure is also a SecondaryIndexIdentifiable, return this as the only item
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		
		/* SecondaryIndexIdentifiable Methods */
		
		// Since Treasure is also a ResourceIdentifiable, return this instead of creating a new instance
		public SecondaryIndexIdentifiable construct(ResourceIdentifiable resourceIdentifiable) {
			if (this != resourceIdentifiable)
				throw new RuntimeException("Expected equality here");
			return this;
		}
		public Integer getSecondaryIndexKey() { return id; }
		public ResourceIdentifiable getResourceIdentifiable() {
			return this;
		}
		public String getSecondaryIndexColumnName() { return "id"; }
		public String getSecondaryIndexName() {return getResourceName() + "." + getSecondaryIndexColumnName(); }		
	}	                      
}
