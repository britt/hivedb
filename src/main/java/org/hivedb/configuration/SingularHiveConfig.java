package org.hivedb.configuration;



/**
 *  An interface representing the configuration of a class that is represented as a resource in the hive.
 * @author andylikuski
 *
 */
public interface SingularHiveConfig extends HiveConfig {
	EntityConfig getEntityConfig();
}
