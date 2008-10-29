package org.hivedb.util;

public interface Identifiable<F> {
	F getId();
	void setId(F field);
}
