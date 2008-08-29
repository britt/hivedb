package org.hivedb.util;

import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.PairIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestPairIterator {
	@Test
	public void test() {
		
		Assert.assertEquals(Filter.grep(new Filter.TruePredicate() {
		}, new PairIterator<Object>(Arrays.asList(new Object[] {1,2,3,4}))).size()
				, 2);
	}
}
