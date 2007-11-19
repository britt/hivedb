package org.hivedb.util;

import java.util.Map;

import org.hibernate.mapping.PersistentClass;
import org.hibernate.tuple.DynamicMapInstantiator;
import org.hibernate.tuple.Instantiator;
import org.hibernate.tuple.entity.DynamicMapEntityTuplizer;
import org.hibernate.tuple.entity.EntityMetamodel;

public class DynamicTuplizer extends DynamicMapEntityTuplizer {
  
	protected DynamicTuplizer(EntityMetamodel entityMetamodel, PersistentClass persistentClass) {
		super(entityMetamodel, persistentClass);
	}

	// override the buildInstantiator() method to plug in our custom map...
    protected final Instantiator buildInstantiator(
            org.hibernate.mapping.PersistentClass mappingInfo) {
        return new CustomMapInstantiator( mappingInfo );
    }

    private static final class CustomMapInstantiator
            extends DynamicMapInstantiator {
    	PersistentClass mappingInfo;
    	public CustomMapInstantiator(PersistentClass mappingInfo) {
    		this.mappingInfo = mappingInfo;
    	}
	    protected final Map generateMap() {
	    	return ((PropertySetter)Interceptor.newInstance(mappingInfo.getMappedClass())).getAsMap();
	    }
    }
}
