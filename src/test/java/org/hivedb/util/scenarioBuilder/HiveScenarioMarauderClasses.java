package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;

public class HiveScenarioMarauderClasses {
	
	public static EntityConfig getPirateConfiguration()
	{
		return EntityConfigImpl.createPartitioningResourceEntity(
				Pirate.class,
				Pirate.class.getSimpleName().toLowerCase(),
				"id",
				(Collection<? extends EntityIndexConfig>)Collections.singletonList(
						new EntityIndexConfigImpl(Pirate.class,"name"))
				);
						
	}
	public static EntityConfig getTreasureConfiguration()
	{
		return EntityConfigImpl.createEntity(
	 			Treasure.class,
				Pirate.class.getSimpleName().toLowerCase(),
				Treasure.class.getSimpleName().toLowerCase(),
				"pirateId",
				"id",
				Arrays.asList(new EntityIndexConfig[] {
					new EntityIndexConfigImpl(Treasure.class,"jewels"),
					new EntityIndexConfigImpl(Treasure.class,"parrots", "id"),
					new EntityIndexConfigImpl(Treasure.class,"price")
				}));					
	}
	
	public interface Pirate
	{
		Integer getId();
		String getName();
	}
		
	public interface Treasure
	{
		Long getId();
		Integer getPirateId();
		Collection<? extends String> getJewels();
		Collection<? extends Parrot> getParrots();
		Integer getPrice();
	}	                      
	
	public interface Parrot
	{
		Short getId();
		Integer getElocution();
	}
}
