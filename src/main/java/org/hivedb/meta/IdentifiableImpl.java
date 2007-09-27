package org.hivedb.meta;

public class IdentifiableImpl<F> implements Identifiable<F> {

	F id;
	public IdentifiableImpl(F id) { this.id = id; }
	
	public F getId() {
		return id;
	}

	public void setId(F id) {
		this.id = id;
	}

}
