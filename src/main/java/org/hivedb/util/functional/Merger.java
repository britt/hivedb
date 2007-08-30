package org.hivedb.util.functional;

public interface Merger<C1,C2,R> {
	 R f(C1 item1, C2 item2, R result);
}
