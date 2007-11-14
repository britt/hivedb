package org.hivedb.util.database.test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.Continent;
import org.hivedb.hibernate.WeatherReport;
import org.hivedb.hibernate.WeatherReportImpl;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeInstaller;
import org.hivedb.meta.NodeInstallerImpl;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Unary;

public class HiveTestCase {
	
	private Unary<String,String> getConnectString;
	private HiveDbDialect hiveDbDialect;
	HiveTestCase(HiveDbDialect hiveDbDialect, Unary<String,String> getConnectString)
	{
		this.getConnectString = getConnectString;
		this.hiveDbDialect = hiveDbDialect;
	}
	public void beforeClass() {
		
	}
	public void beforeMethod() {
		hive = null;
		new HiveInstaller(getConnectString.f(getHiveDatabaseName())).run();
		final EntityHiveConfig entityHiveConfig = getEntityHiveConfig();
		installEntityHiveConfig();
		NodeInstaller installer = new NodeInstallerImpl(entityHiveConfig);
			
		for(String nodeName : this.getDatabaseNames()) 
			if (nodeName != getHiveDatabaseName())
				installer.registerNode(nodeName, getConnectString.f(nodeName));
	}
	
	final ConfigurationReader configurationReader = new ConfigurationReader(Continent.class, WeatherReportImpl.class);
	public EntityHiveConfig getEntityHiveConfig()
	{
		final EntityConfig entityConfig = configurationReader.getEntityConfig(Continent.class.getName());
		final Hive hive = resolveHive(
				entityConfig.getPartitionDimensionName(),
				entityConfig.getPrimaryKeyClass());
		return configurationReader.getHiveConfiguration(hive);
	}
	protected void installEntityHiveConfig() {
		EntityHiveConfig entityHiveConfig = getEntityHiveConfig();
		configurationReader.install(resolveHive(
				entityHiveConfig.getPartitionDimensionName(),
				entityHiveConfig.getPartitionDimensionType()));
	}
	private Hive resolveHive(String partitionDimensionName,
							 Class primaryIndexKeyClass) {
		final Hive hive = getOrCreateHive(
				partitionDimensionName,
				JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKeyClass));
		return hive;
	}

	
	Hive hive = null;
	protected Hive getOrCreateHive(final String dimensionName, final int primaryIndexKeyType) {
		if (hive == null)
			hive = Hive.create(
						getConnectString.f(getHiveDatabaseName()),
						dimensionName,
						primaryIndexKeyType);
		return hive;
	}
	public Hive getHive() { 
		return hive;
	}

	protected Collection<Resource> createResources() {
		ArrayList<Resource> resources = new ArrayList<Resource>();
		resources.add(createResource());
		return resources;
	}
	protected Resource createResource() {
		final Resource resource = new Resource("FOO", Types.INTEGER, false);
		resource.setPartitionDimension(createEmptyPartitionDimension());
		return resource;
	}
	protected SecondaryIndex createSecondaryIndex() {
		SecondaryIndex index = new SecondaryIndex("FOO",java.sql.Types.VARCHAR);
		index.setResource(createResource());
		return index;
	}
	protected SecondaryIndex createSecondaryIndex(int id) {
		SecondaryIndex index = new SecondaryIndex(id, "FOO",java.sql.Types.VARCHAR);
		index.setResource(createResource());
		return index;
	}
	protected Node createNode(String name) {
		return new Node(0, name, name, "", 0, hiveDbDialect);
	}
	protected PartitionDimension createPopulatedPartitionDimension() {
		return new PartitionDimension(Hive.NEW_OBJECT_ID, partitionDimensionName(), Types.INTEGER,
				getConnectString.f(getHiveDatabaseName()), createResources());
	}
	protected PartitionDimension createEmptyPartitionDimension() {
		return new PartitionDimension(Hive.NEW_OBJECT_ID, partitionDimensionName(), Types.INTEGER,
				getConnectString.f(getHiveDatabaseName()), new ArrayList<Resource>());
	}
	protected String partitionDimensionName() {
		return "member";
	}
	protected HiveSemaphore createHiveSemaphore() {
		return new HiveSemaphore(false,54321);
	}
	
	public String getHiveDatabaseName() {
		return "hive";
	}
	public Collection<String> getDatabaseNames() {
		return Collections.singletonList(getHiveDatabaseName());  
	}
}
