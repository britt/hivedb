package org.hivedb.management;

public class Migration implements Comparable{
	private int order = 0;
	private Object primaryIndexKey;
	private String originUri, destinationUri, hiveUri, partitionDimension;
	
	public Migration(Object primaryIndexKey,String partitionDimension, String origin, String destination, String hiveUri) {
		this.primaryIndexKey = primaryIndexKey;
		this.originUri = origin;
		this.destinationUri = destination;
		this.partitionDimension = partitionDimension;
		this.hiveUri = hiveUri;
	}

	public String getDestinationUri() {
		return destinationUri;
	}

	public void setDestinationUri(String destinationUri) {
		this.destinationUri = destinationUri;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public String getOriginUri() {
		return originUri;
	}

	public void setOriginUri(String originUri) {
		this.originUri = originUri;
	}

	public Object getPrimaryIndexKey() {
		return primaryIndexKey;
	}

	public void setPrimaryIndexKey(Object primaryIndexKey) {
		this.primaryIndexKey = primaryIndexKey;
	}

	public int compareTo(Object o) {
		Migration m = (Migration) o;
		if(this.getOrder() != m.getOrder())
			return new Integer(order).compareTo(m.getOrder());
		else
			return new Integer(getPrimaryIndexKey().hashCode()).compareTo(m.getPrimaryIndexKey().hashCode());
	}

	public String getHiveUri() {
		return hiveUri;
	}

	public void setHiveUri(String hiveUri) {
		this.hiveUri = hiveUri;
	}

	public String getPartitionDimension() {
		return partitionDimension;
	}

	public void setPartitionDimension(String partitionDimension) {
		this.partitionDimension = partitionDimension;
	}
}