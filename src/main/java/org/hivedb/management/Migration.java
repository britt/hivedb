package org.hivedb.management;

import org.hivedb.meta.Node;

public class Migration implements Comparable{
	private int order = 0;
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
	public int getOrder() {
		return order;
	}
	public int compareTo(Object o) {
		Migration m = (Migration) o;
		if(this.getOrder() != m.getOrder())
			return new Integer(order).compareTo(m.getOrder());
		else
			return new Integer(getMigrantId().hashCode()).compareTo(o.hashCode());
	}
}