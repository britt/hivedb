package org.hivedb.services;

import javax.jws.WebMethod;

import org.hivedb.services.ServiceContainer;
import org.hivedb.services.ServiceResponse;

public interface Service<T,S extends ServiceResponse<T,C>,C extends ServiceContainer<T>, COL extends Iterable<T>, F> {

}
