package org.hivedb.util;

import java.util.Collection;

import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;

public class GeneratePrimitiveCollection<F> implements Generator<Collection<F>> {

	private Class<F> collectionItemClazz;
	private int size;
	public GeneratePrimitiveCollection(Class<F> collectionItemClazz, int size)
	{
		this.collectionItemClazz = collectionItemClazz;
		this.size = size;
	}
	@SuppressWarnings("unchecked")
	public Collection<F> f() {
		return  Generate.create(new GeneratePrimitiveValue<F>(this.collectionItemClazz), new NumberIterator(size));
	}
}
