package org.hivedb;

import java.util.Collection;

import org.hivedb.meta.Assigner;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.DataSourceProvider;
import org.hivedb.util.database.HiveDbDialect;

public interface HiveFacade {

	public String getUri();

	public DataSourceProvider getDataSourceProvider();

	public boolean isReadOnly();

	public int getRevision();

	public HiveSemaphore getSemaphore();

	public PartitionDimension getPartitionDimension();

	public HiveDbDialect getDialect();

	public Assigner getAssigner();

	public Node addNode(Node node) throws HiveReadOnlyException;

	public Collection<Node> addNodes(Collection<Node> nodes)
			throws HiveReadOnlyException;

	public Resource addResource(Resource resource)
			throws HiveReadOnlyException;

	public SecondaryIndex addSecondaryIndex(Resource resource,
			SecondaryIndex secondaryIndex) throws HiveReadOnlyException;

	public Node deleteNode(Node node) throws HiveReadOnlyException;

	public Resource deleteResource(Resource resource)
			throws HiveReadOnlyException;

	public SecondaryIndex deleteSecondaryIndex(
			SecondaryIndex secondaryIndex) throws HiveReadOnlyException;

	public Collection<Node> getNodes();
}