package org.hivedb.meta;

import org.hivedb.util.ReflectionTools;

public class SecondaryIndexIdentifiableImpl implements SecondaryIndexIdentifiable {
		
	public SecondaryIndexIdentifiableImpl(
			String secondaryIndexKeyPropertyName,
			boolean isManyToOneMultiplicity) {
		this.secondaryIndexKeyPropertyName = secondaryIndexKeyPropertyName;
		this.isManyToOneMultiplicity = isManyToOneMultiplicity;
	}
	
	private String secondaryIndexKeyPropertyName;
	public String getSecondaryIndexKeyPropertyName() {
		return secondaryIndexKeyPropertyName;
	}

	public Object getSecondaryIndexValue(Object resourceInstance) {
		return ReflectionTools.invokeGetter(resourceInstance, secondaryIndexKeyPropertyName);
	}
	
	public boolean isManyToOneMultiplicity;
	public boolean isManyToOneMultiplicity() {
		return isManyToOneMultiplicity;
	}

}
