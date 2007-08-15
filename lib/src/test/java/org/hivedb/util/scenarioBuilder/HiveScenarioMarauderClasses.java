package org.hivedb.util.scenarioBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hivedb.meta.PrimaryIndexIdentifiable;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.SecondaryIndexIdentifiable;

public class HiveScenarioMarauderClasses {
	public static class Pirate implements PrimaryIndexIdentifiable, ResourceIdentifiable, SecondaryIndexIdentifiable
	{
		public static int idGenerator = 0;
		public Pirate() { 
			id = ++idGenerator; //Redundantly named pirates, every other pirate has the same name so that name
			//is a many-to-one sec[ondary index
			pirateName = "name"+(id%2); }		
		protected int id;
		String pirateName;
		public String getSecondaryIndexKey() {	return pirateName;}
		
		/* PrimaryIndexIdentifiable METHODS */
		
		// construct from a prototype instance
		public PrimaryIndexIdentifiable generate() {
			return new Pirate();
		}
		
		public Collection<ResourceIdentifiable> getResourceIdentifiables()
		{
			List<ResourceIdentifiable> list = new ArrayList<ResourceIdentifiable>();
			list.add(this);
			list.add(new Treasure(this));
			return list;
		}
		
		public Integer getPrimaryIndexKey() { return id; }
		public String getPartitionDimensionName() { return this.getClass().getSimpleName().toLowerCase(); }
		
		/* ResourceIdentifiable METHODS */
		
		// Because Pirate is also a PrimaryIndexIdentifiable the argument is equal to this.
		public ResourceIdentifiable generate(PrimaryIndexIdentifiable primaryIndexIdentifiable) {
			
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
		
		@SuppressWarnings("unchecked")
		public String getRepresentedField() {
			return "id";
		}
		
		public Number getId() {
			return id;
		}
		
		public boolean isPartitioningResource() {
			return true;
		}
		
		/* SecondaryIndex METHODS */
		
		// Since Pirate is also a ResourceIdentifiable the argument is equal to this
		public SecondaryIndexIdentifiable generate(ResourceIdentifiable resourceIdentifiable, Object id) {
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
			return getSecondaryIndexColumnName();
		}

		public Object getRepresentedResourceFieldValue() {
			return pirateName;
		}

		public boolean isOneToMany() {
			return true;
		}

		public String getRepresentedFieldName() {
			return "name";
		}

		public boolean isUniqueSecondaryIndex() {
			return false;
		}
	}
	
	public static class Treasure implements ResourceIdentifiable, SecondaryIndexIdentifiable 
	{
		public static int idGenerator = 0;
		protected int id;
		protected int secondaryIndexKey;
		Pirate pirate;

		public Treasure(Pirate pirate) {
			id = ++idGenerator;
			secondaryIndexKey = idGenerator * 10;
			this.pirate = pirate;
		}
		
		/* ResourceIdentifiable Methods */
	
		public ResourceIdentifiable generate(PrimaryIndexIdentifiable pirate) {
			return new Treasure((Pirate)pirate);
		}
		
		public Pirate getPrimaryIndexIdentifiable() { return pirate; }
		public String getResourceName() { return this.getClass().getSimpleName().toLowerCase(); }
	
		// Since Treasure is also a SecondaryIndexIdentifiable, return this as the only item
		public Collection<SecondaryIndexIdentifiable> getSecondaryIndexIdentifiables()
		{
			return Arrays.asList(new SecondaryIndexIdentifiable[] { this });
		}
		
		public Number getId() {
			return id;
		}
		
		/* SecondaryIndexIdentifiable Methods */
		
		// Since Treasure is also a ResourceIdentifiable, return this instead of creating a new instance
		public SecondaryIndexIdentifiable generate(ResourceIdentifiable resourceIdentifiable, Object id) {
			if (this != resourceIdentifiable)
				throw new RuntimeException("Expected equality here");
			return this;
		}
		public Integer getSecondaryIndexKey() { return secondaryIndexKey; }
		public ResourceIdentifiable getResourceIdentifiable() {
			return this;
		}
		public String getSecondaryIndexColumnName() { return "secondaryIndexColumn"; }
		public String getSecondaryIndexName() {return getSecondaryIndexColumnName(); }

		public Object getRepresentedResourceFieldValue() {
			return id;
		}

		@SuppressWarnings("unchecked")
		public String getRepresentedField() {
			return "secondaryIndexKey";
		}

		public boolean isOneToMany() {
			return false;
		}		

		public boolean isUniqueSecondaryIndex() {
			return true;
		}
		
		public boolean isPartitioningResource() {
			return false;
		}
	}	                      
}
