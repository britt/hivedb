package org.hivedb.services;

import javax.jws.WebMethod;
import javax.jws.WebResult;

import org.hivedb.annotations.IndexParamPairs;

public interface Service<T,S extends ServiceResponse<T,C>,C extends ServiceContainer<T>, COL extends Iterable<T>, F> {
}
