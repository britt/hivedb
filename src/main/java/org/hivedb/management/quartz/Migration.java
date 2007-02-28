package org.hivedb.management.quartz;

import org.hivedb.meta.Node;

public class Migration implements Comparable{
	private Object migrant;
	private Node origin, destination;
	
	public Migration(Object migrant,Node origin, Node destination) {
		this.migrant = migrant;
		this.origin = origin;
		this.destination = destination;
	}
	
	public Node getDestination() {
		return destination;
	}
	public Object getMigrantId() {
		return migrant;
	}
	public Node getOrigin() {
		return origin;
	}

	public int compareTo(Object o) {
		return new Integer(getMigrantId().hashCode()).compareTo(o.hashCode());
	}
}