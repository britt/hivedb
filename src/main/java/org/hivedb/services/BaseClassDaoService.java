package org.hivedb.services;

import java.io.Serializable;
import java.util.Collection;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.hibernate.DataAccessObject;

public class BaseClassDaoService implements ClassDaoService {
	protected EntityConfig config;
	protected DataAccessObject<Object, Serializable> dao;
	
	
	public BaseClassDaoService(EntityConfig config, DataAccessObject<Object, Serializable> dao) {
		this.config = config;
		this.dao = dao;
	}

	public Object delete(Serializable id) {
		return dao.delete(id);
	}

	public boolean exists(Serializable id) {
		return dao.exists(id);
	}

	public ServiceResponse get(Serializable id) {
		return new ServiceResponseImpl(config, dao.get(id));
	}

	public ServiceResponse getByReference(String property, Object referenceKey) {
		return new ServiceResponseImpl(config, dao.findByProperty(property, referenceKey));
	}

	public ServiceResponse getByReferenceRange(String property, Object start, Object end) {
		return new ServiceResponseImpl(config, dao.findByPropertyRange(property, start, end));
	}

	@SuppressWarnings("unchecked")
	public Class<Object> getPersistedClass() {
		return (Class<Object>) config.getRepresentedInterface();
	}

	public ServiceResponse save(Object obj) {
		return new ServiceResponseImpl(config, dao.save(obj));
	}

	public ServiceResponse saveAll(Collection<Object> instances) {
		return new ServiceResponseImpl(config, dao.saveAll(instances));
	}

}
