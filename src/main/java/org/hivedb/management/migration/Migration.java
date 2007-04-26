package org.hivedb.management.migration;


public interface Migration {

	public abstract String getDestinationUri();

	public abstract void setDestinationUri(String destinationUri);

	public abstract int getOrder();

	public abstract void setOrder(int order);

	public abstract String getOriginUri();

	public abstract void setOriginUri(String originUri);

	public abstract Object getPrimaryIndexKey();

	public abstract void setPrimaryIndexKey(Object primaryIndexKey);

	public abstract String getHiveUri();

	public abstract void setHiveUri(String hiveUri);

	public abstract String getPartitionDimension();

	public abstract void setPartitionDimension(String partitionDimension);

}