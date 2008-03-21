package org.hivedb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IndexParamPagingPair {
	/**
	 *  Method parameter index value of the start index to use for a paged query
	 * @return
	 */
	int startIndexIndex();
	/**
	 *  Method parameter index value of the max number of results to return for a paged query
	 * @return
	 */
	int maxResultsIndex();
}