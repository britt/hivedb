package org.hivedb.util;

import java.util.Collection;

import org.hivedb.meta.IdAndNameIdentifiable;

public class IdentifiableUtils {
	
	public static boolean isNameUnique(Collection<IdAndNameIdentifiable> collection, String itemName) {
		for (IdAndNameIdentifiable collectionItem : collection)
			if (itemName.equals(collectionItem.getName()))
				return false;
		return true;
	}
	
	/**
	 * Checks if a name is unique in a collection of IdAndNameIdentifiables.  Objects sith the 
	 * same name and id as the filter item are skipped.
	 * 
	 * @param collection  Colelction to search
	 * @param item  filter item to check uniqueness of
	 * @return
	 */
	public static boolean isNameUnique(Collection<IdAndNameIdentifiable> collection, IdAndNameIdentifiable item) {
		String itemName = ((IdAndNameIdentifiable) item).getName();
		for (IdAndNameIdentifiable collectionItem : collection)
			if (itemName.equals((collectionItem).getName()) && collectionItem.getId() != item.getId())
				return false;
		return true;
	}
	
	public static boolean isIdPresent(Collection<IdAndNameIdentifiable> collection, IdAndNameIdentifiable item) {
		for (IdAndNameIdentifiable collectionItem : collection)
			if (item.getId() == collectionItem.getId())
				return true;
		return false;
	}
}
