package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.PrimaryIndexIdentifiableImpl;
import org.hivedb.meta.ResourceIdentifiable;
import org.hivedb.meta.ResourceIdentifiableImpl;
import org.hivedb.meta.SecondaryIndexIdentifiable;
import org.hivedb.meta.SecondaryIndexIdentifiableImpl;


public class HiveScenarioMarauderClasses {
	
	public static ResourceIdentifiable<Object> getPirateConfiguration()
	{
		return new ResourceIdentifiableImpl<Object>(
				Pirate.class,
				Pirate.class.getSimpleName(),
				new PrimaryIndexIdentifiableImpl(Pirate.class.getSimpleName(),"id"),
				(Collection<? extends SecondaryIndexIdentifiable>)Collections.singletonList(new SecondaryIndexIdentifiableImpl("name",false)),
				true,
				"id");
						
	}
	public static ResourceIdentifiable<Object> getTreasureConfiguration()
	{
		return new ResourceIdentifiableImpl<Object>(
				Treasure.class,
				Treasure.class.getSimpleName(),
				new PrimaryIndexIdentifiableImpl(Pirate.class.getSimpleName(),"pirateId"),
				Arrays.asList(new SecondaryIndexIdentifiable[] {
					new SecondaryIndexIdentifiableImpl("jewels",true),
					new SecondaryIndexIdentifiableImpl("price", false)
				}),
				false,
				"id");					
	}
	
	public interface Pirate
	{
		public Integer getId();
		public String getName();
	}
		
	public interface Treasure
	{
		public Long getId();
		public Integer getPirateId();
		public Collection<? extends String> getJewels();
		public Integer getPrice();
	}	                      
}
