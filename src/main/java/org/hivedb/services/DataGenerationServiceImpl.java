package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.jws.WebService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hibernate.shards.util.Lists;
import org.hibernate.shards.util.Maps;
import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.annotations.Resource;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.BaseDataAccessObject;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.hibernate.EntityResolver;
import org.hivedb.hibernate.HiveSessionFactory;
import org.hivedb.hibernate.HiveSessionFactoryBuilderImpl;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratePrimitiveValue;
import org.hivedb.util.PrimitiveUtils;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;

@WebService(endpointInterface = "org.hivedb.services.DataGenerationService")
public class DataGenerationServiceImpl implements DataGenerationService {

	private ConfigurationReader config;
	private Map<Class, DataAccessObject<Object, Serializable>> daos = Maps.newHashMap();
	private Log log = LogFactory.getLog(DataGenerationServiceImpl.class);

	public DataGenerationServiceImpl(Collection<Class<?>> classes, HiveFacade hive) {
		config = new ConfigurationReader(classes);
		List<Class<?>> hiveSessionClasses = Lists.newArrayList();
		hiveSessionClasses.addAll(new EntityResolver(config.getHiveConfiguration()).getEntityClasses());
		HiveSessionFactory factory = new HiveSessionFactoryBuilderImpl(config.getHiveConfiguration(), classes, hive, new SequentialShardAccessStrategy());
		for (Class clazz : hiveSessionClasses) {
			if (clazz.getAnnotation(Resource.class) != null) {
				daos.put(clazz, new BaseDataAccessObject(config.getEntityConfig(clazz.getName()),hive,factory));
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.services.DataGenerationService#listClasses()
	 */
	public Collection<String> listClasses() {
		return Transform.map(new Unary<EntityConfig, String>(){
			public String f(EntityConfig item) {
				return item.getRepresentedInterface().getName();
			}}, config.getConfigurations());
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.services.DataGenerationService#generate(java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	public Collection<Long> generate(String clazz, Integer partitionKeyCount, Integer instanceCount) {
		Collection<Long> ids = Lists.newArrayList();
		try {
		EntityConfig entityConfig = config.getEntityConfig(clazz);		
		Generator<Object> pKeyGenerator;
		if(PrimitiveUtils.isPrimitiveClass(entityConfig.getPrimaryKeyClass()))
			pKeyGenerator = new GeneratePrimitiveValue<Object>((Class<Object>) entityConfig.getPrimaryKeyClass());
		else
			pKeyGenerator = new GenerateInstance<Object>((Class<Object>) entityConfig.getPrimaryKeyClass());
		for(int i=0; i<partitionKeyCount; i++) {
			Object pkey = pKeyGenerator.generate();
			DataAccessObject<Object, Serializable> dao = daos.get(entityConfig.getRepresentedInterface());
			GenerateInstance<Object> instanceGenerator = new GenerateInstance<Object>((Class<Object>) entityConfig.getRepresentedInterface());
			for(int j=0; j<instanceCount; j++){
				Object instance = instanceGenerator.generate();
				ReflectionTools.invokeSetter(instance, entityConfig.getPrimaryIndexKeyPropertyName(), pkey);
				dao.save(instance);
				Serializable id = entityConfig.getId(instance);
				if(Number.class.isAssignableFrom(id.getClass())) {
					ids.add(Long.parseLong(id.toString()));
				} else
					throw new UnsupportedOperationException("This implementation can only generate classes with numeric ids.");
			}
		}
		} catch(RuntimeException e) {
			log.fatal(e);
			log.fatal(e.getMessage());
			throw e;
		}
		return ids;
	}
}
