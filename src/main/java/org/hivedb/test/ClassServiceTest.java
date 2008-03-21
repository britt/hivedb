package org.hivedb.test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.cxf.Bus;
import org.apache.cxf.BusException;
import org.apache.cxf.BusFactory;
import org.apache.cxf.aegis.databinding.AegisDatabinding;
import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.binding.soap.SoapBindingFactory;
import org.apache.cxf.binding.soap.SoapTransportFactory;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.apache.cxf.jaxws.JaxWsServerFactoryBean;
import org.apache.cxf.transport.ConduitInitiatorManager;
import org.apache.cxf.transport.DestinationFactoryManager;
import org.apache.cxf.transport.local.LocalTransportFactory;
import org.apache.cxf.wsdl.WSDLManager;
import org.apache.cxf.wsdl11.WSDLManagerImpl;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.HiveRuntimeException;
import org.hivedb.Schema;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.services.Service;
import org.hivedb.services.ServiceContainer;
import org.hivedb.services.ServiceResponse;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.GeneratedClassFactory;
import org.hivedb.util.GeneratedImplementation;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.Lists;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Pair;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public abstract class ClassServiceTest<T,S> extends H2TestCase {

	protected Class<T> clazz;
	protected Class serviceClass;
	protected Class responseClass;
	protected Class containerClass;
	protected String serviceUrl;
	protected Service client;
	protected Service server;
	protected Hive hive;
	protected EntityHiveConfig config;
	protected HiveSessionFactory factory;
	
	public ClassServiceTest(Class<T> clazz, Class serviceClass, Class responseClass, Class containerClass, String serviceUrl) {
		this.clazz = clazz;
		this.serviceClass = serviceClass;
		this.responseClass = responseClass;
		this.containerClass = containerClass;
		this.serviceUrl = serviceUrl;
	}
	
	private boolean init; // can't figure out how to do this with testng
	public void init() {
		if (init)
			return;
		init = true;
		
		this.cleanupAfterEachTest = false;
		ConfigurationReader reader = new ConfigurationReader(ConfigurationReader.extractPartitionDimension(clazz));
		reader.install(getConnectString(getHiveDatabaseName()));
		hive = getHive();
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();
		for(String nodeName : getDataNodeNames())
			try {
				hive.addNode(new Node(nodeName, nodeName, "" , HiveDbDialect.H2));
			}
			catch (HiveLockableException e) {
				throw new HiveRuntimeException("Hive was read-only", e);
			}
	}
	@BeforeClass
	public void setup() throws Exception {
		init();
		Collection<Class> classes = Lists.newArrayList();
		classes.addAll(getEntityClasses());
		ConfigurationReader reader = new ConfigurationReader(getEntityClasses());
		reader.install(hive);
		for(String nodeName : getDataNodeNames())
			for(Schema s : getSchemata())
				s.install(getConnectString(nodeName));
		config = reader.getHiveConfiguration();
		factory = getSessionFactory();
		server = createService(reader);
		startServer(server);
	}
	
	@BeforeMethod
	public void beforeMethod() {
		for(String nodeName : getDataNodeNames())
			for(Schema s : getSchemata()) 
				s.emptyTables(getConnectString(nodeName));
	}
	
	protected abstract Service createService(ConfigurationReader reader);
	
	@Test(groups={"service"})
	public void saveAndRetrieve() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		validate(createServiceResponse(Arrays.asList(instance)), invoke(getClient(), "get", entityConfig.getId(instance)));
	}
	protected ServiceResponse invokeWithArrayArgument(Service client, String methodName, T[] args) {
		try {
			Method method = client.getClass().getMethod(methodName, args.getClass());
			return (ServiceResponse) method.invoke(client, new Object[] {args});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	protected ServiceResponse invoke(Service client, String methodName, Object... args) {
		try {
			Collection<Class> argClasses = Transform.map(new Unary<Object, Class>() {
				public Class f(Object arg) {
					return (arg instanceof GeneratedImplementation) ? ((GeneratedImplementation)arg).retrieveUnderlyingInterface() : arg.getClass();
				}
			}, Arrays.asList(args));
			Class[] argClassArray = new Class[argClasses.size()];
			argClasses.toArray(argClassArray);
				
			Method method = client.getClass().getMethod(methodName, argClassArray);
			return (ServiceResponse) method.invoke(client, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	protected boolean invokeExists(Service client, String method, Object arg) {
		try {
			return (Boolean) client.getClass().getMethod(method, new Class[] {arg.getClass()}).invoke(client, new Object[] {arg});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test(groups={"service"})
	public void saveAll() throws Exception {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Collection<T> instances = Lists.newArrayList();
		for(int i=0; i<5; i++) {
			final T instance = getInstance();
			instances.add(instance);
		}
		Service s = getClient();
		invokeWithArrayArgument(s, "saveAll", collectionToArray(clazz, instances));
		for(Object original : instances)
			validate(
					createServiceResponse(Arrays.asList(original)),
					invoke(s, "get", entityConfig.getId(original)));
	}
	
	@Test(groups={"service"})
	public void delete() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		final Serializable id = entityConfig.getId(instance);
		validate(createServiceResponse(Arrays.asList(instance)), invoke(s, "get",id));
		invoke(s, "delete", id);
		AssertJUnit.assertFalse(invokeExists(s, "exists", id));
	}
	
	@Test(groups={"service"})
	public void exists() throws Exception {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		AssertJUnit.assertFalse(invokeExists(s,"exists",new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getIdClass()).generate()));
		// Make sure that the service generates an empty response when fetching missing items
		AssertJUnit.assertEquals(0, invoke(s, "get", new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getIdClass()).generate()).getContainers().size());
		Object p = getPersistentInstance();
		AssertJUnit.assertTrue(invokeExists(s, "exists", entityConfig.getId(p)));
	}

	@Test(groups={"service"})
	public void findByProperty() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		for (EntityIndexConfig entityIndexConfig : entityConfig.getEntityIndexConfigs()) {
			validate(
					createServiceResponse(Arrays.asList(instance)),
					invoke(
							client, 
							"findByProperty", 
							entityIndexConfig.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig.getIndexValues(instance)).toString()));
		}
	}
	
	
	abstract protected Collection<Schema> getSchemata();
	abstract protected List<Class<?>> getEntityClasses();
	
	protected void validate(ServiceResponse expected, ServiceResponse actual) {
		assertEquals(expected.getContainers().size(), actual.getContainers().size());		
		Map<Object, ServiceContainer> expectedMap = getInstanceHashCodeMap(expected);
		Map<Object, ServiceContainer> actualMap = getInstanceHashCodeMap(actual);

		for(Object key : actualMap.keySet()) {
			assertTrue(
					String.format("Expected results did not contian a ServiceContainer with hashCode %s", key), 
					expectedMap.containsKey(key));
			validate(expectedMap.get(key), actualMap.get(key));
		}
	}
	
	protected void validate(ServiceContainer expected, ServiceContainer actual) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		AssertJUnit.assertEquals(expected.getVersion(), actual.getVersion());
		AssertJUnit.assertEquals(
				ReflectionTools.getDifferingFields(expected.getInstance(), actual.getInstance(), (Class<Object>)clazz).toString(),
				expected.getInstance().hashCode(), 
				actual.getInstance().hashCode());
		AssertJUnit.assertEquals(entityConfig.getId(expected.getInstance()), entityConfig.getId(actual.getInstance()));
		//AssertJUnit.assertEquals(expected.getInstance().getDescription(), actual.getInstance().getDescription());
	}
	
	@BeforeClass
	public void initializeSOAPLocalTransport() throws BusException {		
		String[] transports = new String[]{
				"http://cxf.apache.org/transports/local",
				"http://schemas.xmlsoap.org/soap/http",
				"http://schemas.xmlsoap.org/wsdl/soap/http"
			};
			LocalTransportFactory local = new LocalTransportFactory();
			local.setTransportIds(Arrays.asList(transports));
			
			Bus bus = BusFactory.newInstance().createBus();
			SoapBindingFactory bindingFactory = new SoapBindingFactory();
			bindingFactory.setBus(bus);
			bus.getExtension(BindingFactoryManager.class).registerBindingFactory("http://schemas.xmlsoap.org/wsdl/soap/", bindingFactory);
			bus.getExtension(BindingFactoryManager.class).registerBindingFactory("http://schemas.xmlsoap.org/wsdl/soap/http", bindingFactory);
			
			DestinationFactoryManager dfm = bus.getExtension(DestinationFactoryManager.class);
			
			SoapTransportFactory soap = new SoapTransportFactory();
			soap.setBus(bus);

			dfm.registerDestinationFactory("http://schemas.xmlsoap.org/wsdl/soap/", soap);
			dfm.registerDestinationFactory("http://schemas.xmlsoap.org/soap/", soap);
			dfm.registerDestinationFactory("http://cxf.apache.org/transports/local", soap);
			
			LocalTransportFactory localTransport = new LocalTransportFactory();
			dfm.registerDestinationFactory("http://schemas.xmlsoap.org/soap/http", localTransport);
			dfm.registerDestinationFactory("http://schemas.xmlsoap.org/wsdl/soap/http", localTransport);
			dfm.registerDestinationFactory("http://cxf.apache.org/bindings/xformat", localTransport);
			dfm.registerDestinationFactory("http://cxf.apache.org/transports/local", localTransport);

			ConduitInitiatorManager extension = bus.getExtension(ConduitInitiatorManager.class);
			extension.registerConduitInitiator(LocalTransportFactory.TRANSPORT_ID, localTransport);
			extension.registerConduitInitiator("http://schemas.xmlsoap.org/wsdl/soap/", localTransport);
			extension.registerConduitInitiator("http://schemas.xmlsoap.org/soap/http", localTransport);
			extension.registerConduitInitiator("http://schemas.xmlsoap.org/soap/", localTransport);
			
			WSDLManagerImpl manager = new WSDLManagerImpl();
			manager.setBus(bus);
			bus.setExtension(manager, WSDLManager.class);
	}
	
	public JaxWsServerFactoryBean startServer(Service service) {
		JaxWsServerFactoryBean sf = new JaxWsServerFactoryBean();
		sf.setAddress(serviceUrl);
		sf.setServiceBean(service);
		sf.setServiceClass(service.getClass());
		sf.setDataBinding(new AegisDatabinding());
		sf.getServiceFactory().setDataBinding(new AegisDatabinding());
		sf.create().start();
		return sf;
	}
	
	
	
	protected Object getPersistentInstance() {
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(getClient().getClass(), serviceClass));
		return ((ServiceContainer)Atom.getFirstOrThrow(invoke(getClient(), "save", getInstance()).getContainers())).getInstance();
	}
	protected T getInstance() {
		return new GenerateInstance<T>(clazz).generate();
	}
	
	protected Service getClient() {
		if(client == null)
			client = createClient();
		return client;
	}
	
	public Service createClient() {
		JaxWsProxyFactoryBean factory = new JaxWsProxyFactoryBean();
		factory.setServiceClass(serviceClass);
		factory.setAddress(serviceUrl);
		factory.setDataBinding(new AegisDatabinding());
		factory.getServiceFactory().setDataBinding(new AegisDatabinding());
		return (Service)factory.create();
	}

	protected Map<Object, ServiceContainer> getInstanceHashCodeMap(ServiceResponse expected) {
		final GenerateInstance<T> g = new GenerateInstance<T>(clazz);
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		return Transform.toMap(
		new Unary<ServiceContainer, Object>(){
			public Object f(ServiceContainer item) {
				return entityConfig.getId(item.getInstance());
			}},
	
		new Unary<ServiceContainer, ServiceContainer>(){
			public ServiceContainer f(ServiceContainer item) {
				return createServiceContainer(g.generateAndCopyProperties(item.getInstance()), item.getVersion());
			}}, 
		expected.getContainers());
	}

	@Override
	public Collection<String> getDatabaseNames() {
		Collection<String> dbs = Lists.newArrayList();
		dbs.addAll(getDataNodeNames());
		if(!dbs.contains(getHiveDatabaseName()))
			dbs.add(getHiveDatabaseName());
		return dbs;
	}

	
	private Collection<String> getDataNodeNames() {
		return Collections.singletonList(getHiveDatabaseName());
	}

	public String getHiveDatabaseName() {
		return "hive";
	}	
	
	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
	
	private HiveSessionFactory getSessionFactory() {
		return new HiveSessionFactoryBuilderImpl(
				getConnectString(getHiveDatabaseName()), 
				getEntityClasses(), 
				new SequentialShardAccessStrategy());
	}
	
	public ServiceResponse createServiceResponse(Collection instances) {
		ServiceResponse serviceResponse = (ServiceResponse) GeneratedClassFactory.newInstance(responseClass);
		GeneratedInstanceInterceptor.setProperty(serviceResponse, "containers", Transform.map(new Unary<Object, ServiceContainer>(){
			public ServiceContainer f(Object item) {
				return createServiceContainer(item, config.getEntityConfig(clazz).getVersion(item));
			}}, instances));
		return serviceResponse;
	}
	public ServiceContainer createServiceContainer(Object instance, Integer version) {
		ServiceContainer serviceContainer = (ServiceContainer) GeneratedClassFactory.newInstance(containerClass);
		GeneratedInstanceInterceptor.setProperty(serviceContainer, "instance", instance); 
		GeneratedInstanceInterceptor.setProperty(serviceContainer, "version", version);
		return serviceContainer;
	}
	protected T[] collectionToArray(Class<T> clazz, Collection c) {
		T[] array = (T[]) Array.newInstance(clazz, c.size());
		c.toArray(array);
		return array;
	}
}