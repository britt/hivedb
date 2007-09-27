package org.hivedb.meta;

public interface Identifiable<F> {
	F getId();
	void setId(F field);
}
