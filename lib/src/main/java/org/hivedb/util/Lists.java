package org.hivedb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class Lists {
	public static<T> List<T> newList(T... items) {
		return Arrays.asList(items);
	}
	
	public static<T> T random(Collection<T> items) {
		int pick = new Random().nextInt(items.size());
		List<T> list = new ArrayList<T>(items);
		return list.get(pick);
	}
}
