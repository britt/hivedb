package org.hivedb.util.classgen;

import org.hivedb.util.functional.Generate;
import org.hivedb.util.functional.Generator;
import org.hivedb.util.functional.NumberIterator;

import java.util.Collection;

public class GeneratePrimitiveCollection<F> implements Generator<Collection<F>> {

	private Class<F> collectionItemClazz;
	private int size;
	public GeneratePrimitiveCollection(Class<F> collectionItemClazz, int size)
	{
		this.collectionItemClazz = collectionItemClazz;
		this.size = size;
	}
	@SuppressWarnings("unchecked")
	public Collection<F> generate() {
		return  Generate.create(new GeneratePrimitiveValue<F>(this.collectionItemClazz), new NumberIterator(size));
	}
}
