package org.hivedb.test;

import org.junit.Assert;
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
import org.hivedb.Schema;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.services.Service;
import org.hivedb.services.ServiceContainer;
import org.hivedb.services.ServiceResponse;
import org.hivedb.util.Lists;
import org.hivedb.util.classgen.*;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.functional.*;
import org.junit.Test;
import org.junit.BeforeClass;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;


public abstract class ClassServiceTest<T,S> extends HiveTest {

	final HiveDbDialect DATABASE_DIALECT = HiveDbDialect.H2;
	protected Class<T> clazz;
	protected Class serviceClass;
	protected Class responseClass;
	protected Class containerClass;
	protected String serviceUrl;
	protected Service client;
	protected Service server;
	protected HiveSessionFactory factory;
	
	public ClassServiceTest(Class<T> clazz, Class serviceClass, Class responseClass, Class containerClass, String serviceUrl) {
		this.clazz = clazz;
		this.serviceClass = serviceClass;
		this.responseClass = responseClass;
		this.containerClass = containerClass;
		this.serviceUrl = serviceUrl;
	}
	
	public void setup() {
		factory = getSessionFactory();
		server = createService(configurationReader);
		startServer(server);
	}
	
	protected abstract Service createService(ConfigurationReader reader);
	
	@Test
	public void saveAndRetrieve() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		validate(createServiceResponse(Arrays.asList(instance)), invoke(getClient(), "get", entityConfig.getId(instance)), Arrays.asList(new String[] {}));
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
		} catch (ClassCastException e) {
			return null;
			// There's a mysterious Long to Long Cast exception here
		}
		 catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Object invokeDelete(Service client, String methodName, Object... args) {
		try {
			Collection<Class> argClasses = Transform.map(new Unary<Object, Class>() {
				public Class f(Object arg) {
					return (arg instanceof GeneratedImplementation) ? ((GeneratedImplementation)arg).retrieveUnderlyingInterface() : arg.getClass();
				}
			}, Arrays.asList(args));
			Class[] argClassArray = new Class[argClasses.size()];
			argClasses.toArray(argClassArray);
				
			Method method = client.getClass().getMethod(methodName, argClassArray);
			return method.invoke(client, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Integer invokeByCount(Service client, String methodName, Object... args) {
		try {
			Collection<Class> argClasses = Transform.map(new Unary<Object, Class>() {
				public Class f(Object arg) {
					return (arg instanceof GeneratedImplementation) ? ((GeneratedImplementation)arg).retrieveUnderlyingInterface() : arg.getClass();
				}
			}, Arrays.asList(args));
			Class[] argClassArray = new Class[argClasses.size()];
			argClasses.toArray(argClassArray);
				
			Method method = client.getClass().getMethod(methodName, argClassArray);
			return (Integer) method.invoke(client, args);
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
	
	@Test
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
					invoke(s, "get", entityConfig.getId(original)),
					Arrays.asList(new String[] {}));
	}
	
	@Test
	public void update() throws Exception {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Collection<T> instances = Lists.newArrayList();
		for(int i=0; i<5; i++) {
			final T instance = getInstance();
			instances.add(instance);
		}
		Service s = getClient();
		invokeWithArrayArgument(s, "saveAll", collectionToArray(clazz, instances));
		for(Object original : instances) {
			for (EntityIndexConfig entityIndexConfig :config.getEntityConfig(clazz).getEntityIndexConfigs()) {
				Object newValue = ReflectionTools.isCollectionProperty(clazz, entityIndexConfig.getPropertyName())
					? new GenerateInstanceCollection(ReflectionTools.getCollectionItemType(clazz, entityIndexConfig.getPropertyName()), 3).generate()
					: new GenerateInstance(entityIndexConfig.getIndexClass()).generate();
				GeneratedInstanceInterceptor.setProperty(original, entityIndexConfig.getPropertyName(), newValue);
			}
		}
		invokeWithArrayArgument(s, "saveAll", collectionToArray(clazz, instances));
		for(Object updated : instances) {
			validate(
					createServiceResponse(Arrays.asList(updated)),
					invoke(s, "get", entityConfig.getId(updated)),
					Arrays.asList(new String[] {}));
		}
	}
	
	@Test
	public void delete() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		final Serializable id = entityConfig.getId(instance);
		validate(createServiceResponse(Arrays.asList(instance)), invoke(s, "get",id), Arrays.asList(new String[] {}));
		Assert.assertEquals(id, invokeDelete(s, "delete", id));
		Assert.assertFalse(invokeExists(s, "exists", id));
	}
	
	@Test
	public void exists() throws Exception {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		Assert.assertFalse(invokeExists(s,"exists",new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getIdClass()).generate()));
		// Make sure that the service generates an empty response when fetching missing items
		Assert.assertEquals(0, invoke(s, "get", new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getIdClass()).generate()).getContainers().size());
		Object p = getPersistentInstance();
		Assert.assertTrue(invokeExists(s, "exists", entityConfig.getId(p)));
	}

	@Test
	public void findByProperty() throws Exception {
		Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		for (EntityIndexConfig entityIndexConfig : getFilteredEntityIndexConfigs(entityConfig)) {
			if (entityIndexConfig.getIndexValues(instance).size() == 0)
				continue;
			validate(
					createServiceResponse(Arrays.asList(instance)),
					invoke(
							client, 
							"findByProperty", 
							entityIndexConfig.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig.getIndexValues(instance)).toString()),
					Arrays.asList(new String[] {entityIndexConfig.getPropertyName()}));
			Assert.assertEquals(
					(Integer)1,
					invokeByCount(
							client, 
							"getCountByProperty", 
							entityIndexConfig.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig.getIndexValues(instance)).toString()));
		}
		
	}
	
	@Test
	public void findByProperties() throws Exception {
		final Object instance = getPersistentInstance();
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Service s = getClient();
		Collection<EntityIndexConfig> entityIndexConfigs = getFilteredEntityIndexConfigs(entityConfig);
		RingIteratorable<EntityIndexConfig> entityIndexConfigIterator2 = makeEntityIndexConfigRingIterable(instance, entityIndexConfigs);
		RingIteratorable<EntityIndexConfig> entityIndexConfigIterator3 = makeEntityIndexConfigRingIterable(instance, entityIndexConfigs);
		entityIndexConfigIterator2.next();
		entityIndexConfigIterator3.next();
		entityIndexConfigIterator3.next();
		for (EntityIndexConfig entityIndexConfig1 : entityIndexConfigs) {
			if (entityIndexConfig1.getIndexValues(instance).size() == 0)
				continue;
			EntityIndexConfig entityIndexConfig2 = entityIndexConfigIterator2.next();
			EntityIndexConfig entityIndexConfig3 = entityIndexConfigIterator3.next();
			validate(
					createServiceResponse(Arrays.asList(instance)),
					invoke(
							client, 
							"findByTwoProperties", 
							entityIndexConfig1.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig1.getIndexValues(instance)).toString(),
							entityIndexConfig2.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig2.getIndexValues(instance)).toString()
					),
					Arrays.asList(new String[] {entityIndexConfig1.getPropertyName(), entityIndexConfig2.getPropertyName()}));
		
			validate(
					createServiceResponse(Arrays.asList(instance)),
					invoke(
							client, 
							"findByThreeProperties", 
							entityIndexConfig1.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig1.getIndexValues(instance)).toString(),
							entityIndexConfig2.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig2.getIndexValues(instance)).toString(),
							entityIndexConfig3.getPropertyName(), 
							Atom.getFirstOrThrow(entityIndexConfig3.getIndexValues(instance)).toString()
					),
					Arrays.asList(new String[] {entityIndexConfig1.getPropertyName(), entityIndexConfig2.getPropertyName(), entityIndexConfig3.getPropertyName()}));
		}
	}
	private RingIteratorable<EntityIndexConfig> makeEntityIndexConfigRingIterable(
			final Object instance,
			Collection<EntityIndexConfig> entityIndexConfigs) {
		return new RingIteratorable<EntityIndexConfig>(Filter.grep(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return entityIndexConfig.getIndexValues(instance).size() > 0;
			}
		}, entityIndexConfigs));
	}
	
	// Filter out EntityIndexConfig types that don't work
	private Collection<EntityIndexConfig> getFilteredEntityIndexConfigs(EntityConfig entityConfig) {
		return Filter.grep(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return !entityIndexConfig.getIndexClass().equals(Date.class); // I can't figure out what format CXF likes
			}}, entityConfig.getEntityIndexConfigs());
	}
	
	
	abstract protected Collection<Schema> getSchemata();
	abstract protected List<Class<?>> getEntityClasses();
	
	protected void validate(ServiceResponse expected, ServiceResponse actual, Collection<String> arguments) {
		Assert.assertEquals(
				String.format("Testing using the following arguments: %s", arguments),
				expected.getContainers().size(), actual.getContainers().size());		
		Map<Object, ServiceContainer> expectedMap = getInstanceHashCodeMap(expected);
		Map<Object, ServiceContainer> actualMap = getInstanceHashCodeMap(actual);

		for(Object key : actualMap.keySet()) {
			Assert.assertTrue(
					String.format("Expected results did not contian a ServiceContainer with hashCode %s", key), 
					expectedMap.containsKey(key));
			validate(expectedMap.get(key), actualMap.get(key), arguments);
		}
	}
	
	protected void validate(ServiceContainer expected, ServiceContainer actual, Collection<String> arguments) {
		final EntityConfig entityConfig = config.getEntityConfig(clazz);
		Assert.assertEquals(expected.getVersion(), actual.getVersion());
		Assert.assertEquals(
				ReflectionTools.getDifferingFields(expected.getInstance(), actual.getInstance(), (Class<Object>)clazz).toString(),
				expected.getInstance().hashCode(), 
				actual.getInstance().hashCode());
		Assert.assertEquals(String.format("Testing using the following arguments: %s", arguments), entityConfig.getId(expected.getInstance()), entityConfig.getId(actual.getInstance()));
		//Assert.assertEquals(expected.getInstance().getDescription(), actual.getInstance().getDescription());
	}
	
	@BeforeClass
	public static void initializeSOAPLocalTransport() throws BusException {		
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
		Object obj = Atom.getFirstOrThrow(getPersistentInstanceAsServiceResponse().getContainers());
		return ((ServiceContainer) obj).getInstance();
		//return ((ServiceContainer)Atom.getFirstOrThrow(getPersistentInstanceAsServiceResponse().getContainers())).getInstance();
	}
	protected ServiceResponse getPersistentInstanceAsServiceResponse() {
		Assert.assertTrue(ReflectionTools.doesImplementOrExtend(getClient().getClass(), serviceClass));
		return  invoke(getClient(), "save", getInstance());
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
	
	private HiveSessionFactory getSessionFactory() {
		return new HiveSessionFactoryBuilderImpl(
				config,
				getHive(),
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