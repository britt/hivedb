package org.hivedb.util.database.test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Lockable.Status;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.JdbcTypeMapper;
import org.hivedb.util.functional.Unary;

public class HiveTestCase {
	
	private Unary<String,String> getConnectString;
	private HiveDbDialect hiveDbDialect;
	private ConfigurationReader configurationReader;
	private Collection<String> dataNodeNames;
	private String hiveDatabaseName;
	private boolean doInstall;
	HiveTestCase(
			String hiveDatabaseName,
			Collection<Class<?>> entityClasses,
			HiveDbDialect hiveDbDialect, 
			Unary<String,String> getConnectString,
			Collection<String> dataNodeNames,
			boolean doInstall)
	{
		this.hiveDatabaseName = hiveDatabaseName;
		this.getConnectString = getConnectString;
		this.hiveDbDialect = hiveDbDialect;
		this.configurationReader = new ConfigurationReader(entityClasses);
		this.dataNodeNames = dataNodeNames;
		this.doInstall = doInstall;
	}
	public void beforeClass() {
		
	}
	public void beforeMethod() {
		
		if (doInstall) {
			hive = null;
			String connectString = getConnectString.f(getHiveDatabaseName());
			new HiveInstaller(connectString).run();		
			installEntityHiveConfig();
			
			String username = getUserName(connectString);
			String password = getPassword(connectString);
			for(String nodeName : dataNodeNames)
				try {
					Node node = new Node(Hive.NEW_OBJECT_ID,nodeName, nodeName, hiveDbDialect == HiveDbDialect.H2 ? "" : "localhost", hiveDbDialect);
					if (username != null)
						node.setUsername(username);
					else
						node.setUsername("");
					if (password != null)
						node.setPassword(password);
					else
						node.setUsername("");
					hive.addNode(node);
				} catch (HiveLockableException e) {
					throw new HiveRuntimeException("Hive was read-only", e);
				}
		}
		else
			hive = getOrLoadHive();
	}
	
	public EntityHiveConfig getEntityHiveConfig()
	{
		return configurationReader.getHiveConfiguration();
	}
	protected void installEntityHiveConfig() {
		EntityHiveConfig entityHiveConfig = getEntityHiveConfig();
		configurationReader.install(getOrCreateHive(
				entityHiveConfig.getPartitionDimensionName(),
				entityHiveConfig.getPartitionDimensionType()));
	}
	
	Hive hive = null;
	protected Hive getOrCreateHive(final String dimensionName, final Class primaryIndexKeyType) {
		if (hive == null)
			hive = Hive.create(
						getConnectString.f(getHiveDatabaseName()),
						dimensionName,
						JdbcTypeMapper.primitiveTypeToJdbcType(primaryIndexKeyType));
		return hive;
	}
	protected Hive getOrLoadHive() {
		if (hive == null)
			hive = Hive.load(getConnectString.f(getHiveDatabaseName()));
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
		return new Node(0, name, name, "", hiveDbDialect);
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
		return new HiveSemaphore(Status.writable,54321);
	}
	
	public String getHiveDatabaseName() {
		return hiveDatabaseName;
	}
	
	private String getUserName(String connectString) {
		String username = null;
		String[] tokens = connectString.split("user=");
		if (tokens.length > 1) {
			username = tokens[1].split("&")[0];
		}
		return username;
	}
	
	private String getPassword(String connectString) {
		String password = null;
		String[] tokens = connectString.split("password=");
		if (tokens.length > 1) {
			password = tokens[1].split("&")[0];
		}
		return password;
	}
}
