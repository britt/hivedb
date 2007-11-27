/**
 * 
 */
package org.hivedb.hibernate;

import org.hivedb.util.database.test.Continent;

public class AsiaticContinent implements Continent {
	private String name = "Asia";
	private Integer population = 5;
	
	public String getName() {return name;}
	public Integer getPopulation() {return population;}
	public void setName(String name) {this.name = name;}
	public void setPopulation(Integer population) {this.population = population;}
}