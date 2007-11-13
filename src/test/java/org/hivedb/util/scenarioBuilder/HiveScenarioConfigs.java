package org.hivedb.util.scenarioBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.configuration.PluralHiveConfig;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;

public class HiveScenarioConfigs {
	
	@SuppressWarnings("unchecked")
	public static EntityHiveConfig getPirateConfiguration(final Hive hive)
	{
		return new PluralHiveConfig(Transform.toMap(new Entry[] {
			new Pair<String, EntityConfig>(Pirate.class.getSimpleName().toLowerCase(),
					EntityConfigImpl.createPartitioningResourceEntity(
						Pirate.class,
						Pirate.class.getSimpleName().toLowerCase(),
						"id",
						(Collection<? extends EntityIndexConfig>)Collections.singletonList(
								new EntityIndexConfigImpl(Pirate.class,"name"))
						)),			
			new Pair<String, EntityConfig>(Pirate.class.getSimpleName().toLowerCase(),
					EntityConfigImpl.createEntity(
			 			Treasure.class,
						Pirate.class.getSimpleName().toLowerCase(),
						Treasure.class.getSimpleName().toLowerCase(),
						"pirateId",
						"id",
						Arrays.asList(new EntityIndexConfig[] {
							new EntityIndexConfigImpl(Treasure.class,"jewels"),
							new EntityIndexConfigImpl(Treasure.class,"parrots", "id"),
							new EntityIndexConfigImpl(Treasure.class,"price")
						})))}),
			hive);					
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
