package org.hivedb.util;

import org.jmock.core.DynamicMockError;
import org.jmock.core.Invocation;
import org.jmock.core.InvocationMatcher;

class MaximumInvocationCountMatcher implements InvocationMatcher {
	private int limit;
	private int count=0;
	public MaximumInvocationCountMatcher(int times) {
		this.limit = times;
	}
	
	public boolean hasDescription() {
		return false;
	}

	public void invoked(Invocation arg0) {
		count++;
		if(count > limit)
			throw new DynamicMockError(null, arg0, null, "You invoked it too many times. " + count + " > " + limit);
	}

	public boolean matches(Invocation arg0) {
		return true;
	}

	public void verify() {
	}

	public StringBuffer describeTo(StringBuffer arg0) {
		return new StringBuffer();
	}
	
}