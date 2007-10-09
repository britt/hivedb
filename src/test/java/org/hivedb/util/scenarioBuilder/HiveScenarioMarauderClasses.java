package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.EntityConfig;
import org.hivedb.meta.EntityConfigImpl;
import org.hivedb.meta.EntityIndexConfig;
import org.hivedb.meta.EntityIndexConfigImpl;
import org.hivedb.util.AccessorFunction;


public class HiveScenarioMarauderClasses {
	
	public static EntityConfig<Object> getPirateConfiguration()
	{
		return EntityConfigImpl.createPartitioningResourceEntity(
				Pirate.class,
				Pirate.class.getSimpleName().toLowerCase(),
				(Collection<? extends EntityIndexConfig>)Collections.singletonList(
						new EntityIndexConfigImpl(Pirate.class,"name")),
				"id");
						
	}
	public static EntityConfig<Object> getTreasureConfiguration()
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
